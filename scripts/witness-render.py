#!/usr/bin/env python3
r"""Check and render witness graphs (witness format 1).

Reads graph files (`<n>.graph.json`, one graph line), graph stores (JSON Lines whose first line is
`{"witness_store":1,...}`, then one graph line per distinct summary line), or a directory (every
`*.graph.json` in it). Python 3 standard library only; run it with `python3 -I`.

  --check            validate rules G1-G12. G9 recomputes the summary line pi(G) from the graph (words,
                     statuses, porf counts) and compares it with the stored `line` byte for byte; on a
                     mismatch the first differing byte is printed. For a store it also checks that every
                     graph line carries the store's `export` and `run` (iteration aside), `join: null`,
                     `nearest: null`, and that no two graph lines carry the same `line` (rules S1-S4).
  --format F[,F...]  tikz, dot, text: write <stem>.tex, <stem>.dot, <stem>.witness.txt beside the input
                     or in --out-dir; an existing different file is never overwritten.
  --tikz, --dot, --text   shorthands for --format.
  --summary          print, per graph, every thread's word and status (visible threads first).

Every mode validates first; an invalid graph is reported as `<file>[:<line>]: G<n>: <reason>` and is not
rendered. Exit 0 when every graph is valid and every output was written; 1 when the only failures are
G9 mismatches (pi(G) differs from `line`); 2 for any other invalid graph, a refused output or bad usage.

tikz fits --width cm (default 16.5). Label widths are estimated from per-character widths measured in
the A2A report's fonts, plus 10%. The x unit (at most 1 cm) shrinks until the widest panel fits and no
two labels of a row overlap. The font is \scriptsize up to four columns and \tiny beyond; if that is
not enough the font drops to \tiny, then the widest send labels break after their destination. More
than --cols-per-row (default 5) columns wrap onto further rows of panels.

The stem of `X.graph.json` is `X`; the graphs of a store `S` (any name) get the stem `<S without
.jsonl>.<i>`, `i` the 1-based position of the graph line in the store (store line i+1); `--index` selects
graphs of a store by that position.
"""

import argparse
import hashlib
import json
import os
import re
import sys
import unicodedata

VERSION = 1

TOP_KEYS = ["witness", "line", "export", "run", "join", "nearest", "threads"]
EXPORT_ORDER = ["format", "minor", "tool", "program", "size", "visible", "roles", "model",
                "enumeration", "seed", "iterations", "encoding"]
EXPORT_REQUIRED = ["format", "tool", "program", "size", "visible", "model", "enumeration", "encoding"]
ONLINE_FIELDS = ["first_miss_iteration", "first_miss_wall_ms", "uncovered_total"]
RUN_KEYS = ["tool", "tool_commit", "entry", "side", "args", "model", "options", "source",
            "iteration", "producer", "order"]
THREAD_KEYS = ["name", "daemon", "events"]
JOIN_KEYS = ["tool", "tool_commit", "mode", "class", "decide", "against", "against_run"]
DECIDE_KEYS = ["outcome", "k", "k_min", "states", "witness"]
STORE_KEYS = ["witness_store", "export", "run"]
EVENT_KEYS = {
    "BEGIN": [["e"]],
    "TCREATE": [["e", "child"]],
    "SEND": [["e", "to", "v"]],
    "RECV": [["e", "mode", "rf"]],
    "NONDET": [["e", "n", "i"], ["e", "n", "i", "lo"]],
    "TJOIN": [["e", "child"]],
    "END": [["e"]],
    "BLK": [["e"]],
    "ERROR": [["e", "msg"]],
}
LAST_KINDS = ("END", "BLK", "ERROR")
STATUS_OF = {"END": "done", "BLK": "blocked", "ERROR": "err"}
I64_MIN, I64_MAX = -(1 << 63), (1 << 63) - 1


class Invalid(Exception):
    def __init__(self, rule, msg):
        super().__init__(msg)
        self.rule = rule
        self.msg = msg


class Refused(Exception):
    """An output that cannot be produced (overwrite, text form of an unnamed thread, ...)."""


def fail(rule, msg):
    raise Invalid(rule, msg)


# ---------------------------------------------------------------- JSON reading and byte form

def _pairs(pairs):
    d = {}
    for k, v in pairs:
        if k in d:
            raise ValueError("duplicate key %s" % json.dumps(k))
        d[k] = v
    return d


def _no_float(s):
    raise ValueError("floating-point number %s" % s)


def _no_const(s):
    raise ValueError("non-JSON constant %s" % s)


def parse_line(text):
    return json.loads(text, object_pairs_hook=_pairs, parse_float=_no_float,
                      parse_constant=_no_const)


def dumps(v):
    """serde_json's compact form (the P5 string rule)."""
    return json.dumps(v, ensure_ascii=False, separators=(",", ":"))


def json_str(s):
    return json.dumps(s, ensure_ascii=False)


def is_int(v):
    return isinstance(v, int) and not isinstance(v, bool)


def first_diff(a, b):
    """Offset of the first differing character of two strings, and a context of each."""
    n = min(len(a), len(b))
    i = 0
    while i < n and a[i] == b[i]:
        i += 1
    lo = max(0, i - 40)
    off = len(a[:i].encode("utf-8"))
    return off, a[lo:i + 40], b[lo:i + 40]


# ---------------------------------------------------------------- G1: shape and types

def keys_exact(obj, keys, where):
    if not isinstance(obj, dict):
        fail(1, "%s is not an object" % where)
    if list(obj) != keys:
        fail(1, "%s keys %s, expected %s" % (where, list(obj), keys))


def check_export(ex, where="export"):
    if not isinstance(ex, dict):
        fail(1, "%s is not an object" % where)
    for k in ex:
        if k in ONLINE_FIELDS:
            fail(1, "%s holds the online field %s" % (where, k))
        if k not in EXPORT_ORDER:
            fail(1, "%s holds the unknown key %s" % (where, json_str(k)))
    ks = list(ex)
    if ks != [k for k in EXPORT_ORDER if k in ex]:
        fail(1, "%s keys %s are not in header order" % (where, ks))
    for k in EXPORT_REQUIRED:
        if k not in ex:
            fail(1, "%s lacks %s" % (where, k))
    if ex["format"] != 1 or not is_int(ex["format"]):
        fail(1, "%s.format is not 1" % where)
    if "minor" in ex and not is_int(ex["minor"]):
        fail(1, "%s.minor is not an integer" % where)
    if ex.get("minor", 0) >= 1 and "roles" not in ex:
        fail(1, "%s is minor %d and has no roles" % (where, ex["minor"]))
    for k in ("tool", "program", "size", "model", "enumeration"):
        if not isinstance(ex[k], str):
            fail(1, "%s.%s is not a string" % (where, k))
    for k in ("visible", "roles"):
        if k in ex and not (isinstance(ex[k], list) and all(isinstance(x, str) for x in ex[k])):
            fail(1, "%s.%s is not a list of strings" % (where, k))
    for k in ("seed", "iterations", "encoding"):
        if k in ex and not is_int(ex[k]):
            fail(1, "%s.%s is not an integer" % (where, k))


def check_run(run, where="run"):
    keys_exact(run, RUN_KEYS, where)
    if run["tool"] not in ("jmc", "traceforge"):
        fail(1, "%s.tool %s" % (where, json_str(run["tool"])))
    if not isinstance(run["tool_commit"], str):
        fail(1, "%s.tool_commit is not a string" % where)
    if not isinstance(run["entry"], str):
        fail(1, "%s.entry is not a string" % where)
    if run["side"] not in ("spec", "impl", None):
        fail(1, "%s.side %s" % (where, dumps(run["side"])))
    if not (isinstance(run["args"], list) and all(isinstance(a, str) for a in run["args"])):
        fail(1, "%s.args is not a list of strings" % where)
    if not (run["model"] is None or isinstance(run["model"], str)):
        fail(1, "%s.model is not a string or null" % where)
    if not isinstance(run["options"], dict):
        fail(1, "%s.options is not an object" % where)
    src = run["source"]
    if src != "unknown":
        keys_exact(src, ["root", "files"], where + ".source")
        if not isinstance(src["root"], str):
            fail(1, "%s.source.root is not a string" % where)
        files = src["files"]
        if not (isinstance(files, dict) and all(isinstance(v, str) for v in files.values())):
            fail(1, "%s.source.files is not an object of strings" % where)
        fk = list(files)
        if fk != sorted(fk, key=lambda s: s.encode("utf-8")):
            fail(1, "%s.source.files keys are not sorted by bytes" % where)
    it = run["iteration"]
    if not (it is None or (is_int(it) and it >= 1)):
        fail(1, "%s.iteration %s is not a positive integer or null" % (where, dumps(it)))
    if run["producer"] not in ("export", "online", "conf"):
        fail(1, "%s.producer %s" % (where, dumps(run["producer"])))
    if not isinstance(run["order"], str):
        fail(1, "%s.order is not a string" % where)


def check_join(join):
    if join is None:
        return
    if not isinstance(join, dict):
        fail(1, "join is not an object or null")
    for k in JOIN_KEYS:
        if k not in join:
            fail(1, "join lacks %s" % k)
    known = [k for k in join if k in JOIN_KEYS]
    if known != JOIN_KEYS:
        fail(1, "join keys %s are not in order" % known)
    if join["mode"] not in ("plain", "merge", "online"):
        fail(1, "join.mode %s" % dumps(join["mode"]))
    if join["class"] not in ("no-bucket", "no-slot", "not-found", None):
        fail(1, "join.class %s" % dumps(join["class"]))
    d = join["decide"]
    if d is not None:
        keys_exact(d, DECIDE_KEYS, "join.decide")
        if d["outcome"] not in ("violation", "unresolved"):
            fail(1, "join.decide.outcome %s" % dumps(d["outcome"]))
        for k in ("k", "k_min", "states"):
            if not is_int(d[k]):
                fail(1, "join.decide.%s is not an integer" % k)
        w = d["witness"]
        if w is not None and not (isinstance(w, list) and all(
                isinstance(p, list) and len(p) == 2 and isinstance(p[0], str) and is_int(p[1])
                for p in w)):
            fail(1, "join.decide.witness is not a list of [role, index]")
    if join["against"] is not None:
        check_export(join["against"], "join.against")
    if join["against_run"] is not None:
        check_run(join["against_run"], "join.against_run")


def check_nearest(nearest):
    if nearest is None:
        return
    if not isinstance(nearest, list):
        fail(1, "nearest is not a list or null")
    for j, n in enumerate(nearest):
        keys_exact(n, ["line", "graph"], "nearest[%d]" % j)
        if not isinstance(n["line"], str) or not (n["graph"] is None or isinstance(n["graph"], str)):
            fail(1, "nearest[%d] has a wrong type" % j)


def check_event(ev, where):
    if not isinstance(ev, dict) or "e" not in ev:
        fail(1, "%s is not an event object" % where)
    kind = ev["e"]
    if kind not in EVENT_KEYS:
        fail(1, "%s has the unknown kind %s" % (where, dumps(kind)))
    if list(ev) not in EVENT_KEYS[kind]:
        fail(1, "%s (%s) keys %s, expected %s" % (where, kind, list(ev),
                                                  " or ".join(map(str, EVENT_KEYS[kind]))))
    for k in ("child", "to", "n", "i", "lo"):
        if k in ev and not is_int(ev[k]):
            fail(1, "%s.%s is not an integer" % (where, k))
    if kind == "RECV":
        if ev["mode"] not in ("block", "try"):
            fail(1, "%s.mode %s" % (where, dumps(ev["mode"])))
        rf = ev["rf"]
        if rf is not None and not (isinstance(rf, list) and len(rf) == 2 and all(is_int(x) for x in rf)):
            fail(1, "%s.rf is not [t, k] or null" % where)
    if kind == "ERROR" and not isinstance(ev["msg"], str):
        fail(1, "%s.msg is not a string" % where)


# ---------------------------------------------------------------- G8: values

def p5_value(v):
    """True iff v is a P5 interchange encoding."""
    if isinstance(v, bool) or isinstance(v, str):
        return True
    if is_int(v):
        return I64_MIN <= v <= I64_MAX
    if isinstance(v, list):
        return all(p5_value(x) for x in v)
    if isinstance(v, dict):
        return (list(v) == ["t", "f"] and isinstance(v["t"], str) and isinstance(v["f"], list)
                and all(p5_value(x) for x in v["f"]))
    return False


def is_opaque(v):
    return isinstance(v, dict) and list(v) == ["opaque"] and isinstance(v["opaque"], str)


def name_ok(s):
    return not any(unicodedata.category(c) == "Cc" or c in "\u2028\u2029" for c in s)


def r1_name(n):
    return n != "" and n != "main" and not n.startswith("~") and n.isascii()


# ---------------------------------------------------------------- the graph

class Graph:
    """A validated graph and what the renderers need from it."""

    def __init__(self, obj, raw):
        self.obj = obj
        self.raw = raw
        self.line = obj["line"]
        self.sha = hashlib.sha256(self.line.encode("utf-8")).hexdigest()
        self.export = obj["export"]
        self.run = obj["run"]
        self.threads = obj["threads"]
        self.visible = list(self.export["visible"])


def validate(obj, raw):
    """Check G1-G12 on a parsed graph line; return a Graph with pi(G) and porf facts."""
    # G1
    if not isinstance(obj, dict):
        fail(1, "the line is not a JSON object")
    if obj.get("witness") != 1 or not is_int(obj.get("witness")):
        fail(1, "witness is %s, not 1" % dumps(obj.get("witness")))
    keys_exact(obj, TOP_KEYS, "the graph")
    if dumps(obj) != raw:
        off, a, b = first_diff(raw, dumps(obj))
        fail(1, "not in byte form (compact serde_json) at byte %d: %s | expected %s"
             % (off, json_str(a), json_str(b)))
    if not isinstance(obj["line"], str):
        fail(1, "line is not a string")
    check_export(obj["export"])
    check_run(obj["run"])
    check_join(obj["join"])
    check_nearest(obj["nearest"])
    threads = obj["threads"]
    if not isinstance(threads, list):
        fail(1, "threads is not a list")
    for t, th in enumerate(threads):
        keys_exact(th, THREAD_KEYS, "threads[%d]" % t)
        if not (th["name"] is None or isinstance(th["name"], str)):
            fail(1, "threads[%d].name is not a string or null" % t)
        if not isinstance(th["daemon"], bool):
            fail(1, "threads[%d].daemon is not a boolean" % t)
        if not isinstance(th["events"], list):
            fail(1, "threads[%d].events is not a list" % t)
        for k, ev in enumerate(th["events"]):
            check_event(ev, "event (%d, %d)" % (t, k))

    g = Graph(obj, raw)
    ex, run = g.export, g.run
    T = len(threads)
    names = [th["name"] for th in threads]

    # G2
    if T == 0:
        fail(2, "threads is empty")
    if names[0] != "main" or threads[0]["daemon"]:
        fail(2, "thread 0 is not the non-daemon thread \"main\"")
    for x in g.visible:
        c = names.count(x)
        if c != 1:
            fail(2, "visible name %s occurs %d times among the threads" % (json_str(x), c))

    # G3
    for t, th in enumerate(threads):
        evs = th["events"]
        if not evs:
            fail(3, "thread %d has no events" % t)
        if evs[0]["e"] != "BEGIN":
            fail(3, "event (%d, 0) is %s, not BEGIN" % (t, evs[0]["e"]))
        if evs[-1]["e"] not in LAST_KINDS:
            fail(3, "the last event of thread %d is %s" % (t, evs[-1]["e"]))
        for k, ev in enumerate(evs):
            if ev["e"] == "BEGIN" and k != 0:
                fail(3, "BEGIN at (%d, %d)" % (t, k))
            if ev["e"] in LAST_KINDS and k != len(evs) - 1:
                fail(3, "%s at (%d, %d) is not the last event" % (ev["e"], t, k))

    # G4
    creator = {}
    for t, th in enumerate(threads):
        for k, ev in enumerate(th["events"]):
            if ev["e"] == "TCREATE":
                c = ev["child"]
                if not (1 <= c < T):
                    fail(4, "TCREATE (%d, %d) names thread %d" % (t, k, c))
                if c == t:
                    fail(4, "thread %d creates itself" % t)
                if c in creator:
                    fail(4, "thread %d is created twice, at %s and (%d, %d)" % (c, creator[c], t, k))
                creator[c] = (t, k)
    for t in range(1, T):
        if t not in creator:
            fail(4, "thread %d is never created" % t)
    for t, th in enumerate(threads):
        for k, ev in enumerate(th["events"]):
            if ev["e"] == "TJOIN":
                c = ev["child"]
                if not (1 <= c < T) or c == t:
                    fail(4, "TJOIN (%d, %d) names thread %d" % (t, k, c))
                if threads[c]["events"][-1]["e"] != "END":
                    fail(4, "TJOIN (%d, %d) joins thread %d, which does not end with END" % (t, k, c))

    # G5
    read_by = {}
    for t, th in enumerate(threads):
        for k, ev in enumerate(th["events"]):
            if ev["e"] == "SEND" and not (0 <= ev["to"] < T):
                fail(5, "SEND (%d, %d) is to thread %d" % (t, k, ev["to"]))
            if ev["e"] != "RECV":
                continue
            rf = ev["rf"]
            if rf is None:
                if ev["mode"] != "try":
                    fail(5, "RECV (%d, %d) reads null with mode block" % (t, k))
                continue
            ts, ks = rf
            if not (0 <= ts < T and 0 <= ks < len(threads[ts]["events"])):
                fail(5, "RECV (%d, %d) reads from the missing event (%d, %d)" % (t, k, ts, ks))
            sev = threads[ts]["events"][ks]
            if sev["e"] != "SEND":
                fail(5, "RECV (%d, %d) reads from (%d, %d), a %s" % (t, k, ts, ks, sev["e"]))
            if sev["to"] != t:
                fail(5, "RECV (%d, %d) reads SEND (%d, %d), which is to thread %d"
                     % (t, k, ts, ks, sev["to"]))
            if (ts, ks) in read_by:
                fail(5, "SEND (%d, %d) is read twice, by %s and (%d, %d)"
                     % (ts, ks, read_by[(ts, ks)], t, k))
            read_by[(ts, ks)] = (t, k)
    g.read_by = read_by

    # G6: porf acyclic, by Kahn's algorithm; the order is kept for the counts and the rows.
    preds = {}
    succs = {}
    for t, th in enumerate(threads):
        for k in range(len(th["events"])):
            preds[(t, k)] = []
            succs[(t, k)] = []

    def edge(a, b):
        preds[b].append(a)
        succs[a].append(b)

    for t, th in enumerate(threads):
        evs = th["events"]
        for k, ev in enumerate(evs):
            if k > 0:
                edge((t, k - 1), (t, k))
            if ev["e"] == "RECV" and ev["rf"] is not None:
                edge(tuple(ev["rf"]), (t, k))
            if ev["e"] == "TCREATE":
                edge((t, k), (ev["child"], 0))
            if ev["e"] == "TJOIN":
                c = ev["child"]
                edge((c, len(threads[c]["events"]) - 1), (t, k))
    indeg = {e: len(p) for e, p in preds.items()}
    ready = sorted(e for e, d in indeg.items() if d == 0)
    order = []
    while ready:
        e = ready.pop(0)
        order.append(e)
        for s in succs[e]:
            indeg[s] -= 1
            if indeg[s] == 0:
                ready.append(s)
    if len(order) != len(preds):
        stuck = sorted(e for e, d in indeg.items() if d > 0)
        fail(6, "porf has a cycle through %s" % ", ".join("(%d, %d)" % e for e in stuck[:6]))
    g.order = order

    # G7
    for t, th in enumerate(threads):
        for k, ev in enumerate(th["events"]):
            if ev["e"] != "NONDET":
                continue
            if ev["n"] < 2 or not (0 <= ev["i"] < ev["n"]):
                fail(7, "NONDET (%d, %d) has n=%d, i=%d" % (t, k, ev["n"], ev["i"]))
            if "lo" in ev and run["tool"] != "traceforge":
                fail(7, "NONDET (%d, %d) has lo in a %s file" % (t, k, run["tool"]))

    # G8
    vis_tid = {x: names.index(x) for x in g.visible}
    vis_set = set(vis_tid.values())
    for t, th in enumerate(threads):
        for k, ev in enumerate(th["events"]):
            if ev["e"] != "SEND":
                continue
            v = ev["v"]
            if is_opaque(v):
                rd = read_by.get((t, k))
                if t in vis_set or (rd is not None and rd[0] in vis_set):
                    fail(8, "opaque value in SEND (%d, %d), sent or read by a visible thread" % (t, k))
            elif not p5_value(v):
                fail(8, "SEND (%d, %d) value %s is not a P5 encoding" % (t, k, dumps(v)[:80]))

    # G10 (before G9: pi needs the visible names)
    for k in ("visible", "roles"):
        if k not in ex:
            continue
        lst = ex[k]
        for x in lst:
            if not r1_name(x):
                fail(10, "export.%s holds the invalid name %s" % (k, json_str(x)))
        if any(a >= b for a, b in zip(lst, lst[1:])):
            fail(10, "export.%s is not sorted and duplicate-free" % k)
    if "roles" in ex and not set(ex["visible"]) <= set(ex["roles"]):
        fail(10, "export.visible is not contained in export.roles")
    if ex["model"] not in ("p2p", "asyn", "cd"):
        fail(10, "export.model %s" % json_str(ex["model"]))

    # G11
    for t, n in enumerate(names):
        if n is not None and not name_ok(n):
            fail(11, "thread %d's name %s holds a control character or a line separator"
                 % (t, json_str(n)))
    for t, th in enumerate(threads):
        ev = th["events"][-1]
        if ev["e"] == "ERROR" and not name_ok(ev["msg"]):
            fail(11, "the ERROR message of thread %d holds a control character or a line separator" % t)

    # G12
    if run["order"] not in ("first", "smallest"):
        fail(12, "run.order %s" % json_str(run["order"]))
    if run["iteration"] is None and not (
            run["order"] == "smallest"
            or (run["producer"] == "online" and run["tool"] == "traceforge")):
        fail(12, "run.iteration is null with order %s, producer %s" % (run["order"], run["producer"]))

    # G9: the projection.
    project(g, vis_tid)
    if g.pi != g.line:
        off, a, b = first_diff(g.line, g.pi)
        fail(9, "pi(G) differs from line at byte %d: line %s | pi %s"
             % (off, json_str(a), json_str(b)))
    return g


def word_of(g, t):
    """The SEND/RECV word of thread t, as P5 observations, and its status."""
    th = g.threads[t]
    w = []
    for ev in th["events"]:
        if ev["e"] == "SEND":
            w.append({"s": ev["v"]})
        elif ev["e"] == "RECV":
            if ev["rf"] is None:
                w.append({"r": None})
            else:
                ts, ks = ev["rf"]
                w.append({"r": g.threads[ts]["events"][ks]["v"]})
    return w, STATUS_OF[th["events"][-1]["e"]]


def project(g, vis_tid):
    """pi(G) in conf-join's normal form (render_line)."""
    threads = g.threads
    T = len(threads)
    # vector clocks over porf: vc[e][u] = number of events of thread u at or before e.
    vc = {}
    for e in g.order:
        t, k = e
        clock = [0] * T
        for p in preds_of(g, e):
            pc = vc[p]
            for u in range(T):
                if pc[u] > clock[u]:
                    clock[u] = pc[u]
        clock[t] = k + 1
        vc[e] = clock
    g.vc = vc
    # msg_prefix[u][j] = message events among the first j events of thread u.
    msg_prefix = []
    for th in threads:
        acc = [0]
        for ev in th["events"]:
            acc.append(acc[-1] + (1 if ev["e"] in ("SEND", "RECV") else 0))
        msg_prefix.append(acc)
    names = g.visible  # sorted (G10)
    statuses = []
    words = []
    counts = []
    for x in names:
        t = vis_tid[x]
        w, st = word_of(g, t)
        words.append(w)
        statuses.append(st)
        for k, ev in enumerate(threads[t]["events"]):
            if ev["e"] not in ("SEND", "RECV"):
                continue
            for y in names:
                if y == x:
                    continue
                u = vis_tid[y]
                counts.append(msg_prefix[u][vc[(t, k)][u]])
    out = ['{"statuses":{']
    out.append(",".join("%s:%s" % (json_str(x), json_str(s)) for x, s in zip(names, statuses)))
    out.append('},"words":{')
    out.append(",".join("%s:%s" % (json_str(x), dumps(w)) for x, w in zip(names, words)))
    out.append('},"counts":[')
    out.append(",".join(str(c) for c in counts))
    out.append("]}")
    g.pi = "".join(out)


def preds_of(g, e):
    t, k = e
    out = []
    if k > 0:
        out.append((t, k - 1))
    ev = g.threads[t]["events"][k]
    if ev["e"] == "RECV" and ev["rf"] is not None:
        out.append(tuple(ev["rf"]))
    if k == 0 and t > 0:
        for u, th in enumerate(g.threads):
            for j, cev in enumerate(th["events"]):
                if cev["e"] == "TCREATE" and cev["child"] == t:
                    out.append((u, j))
    if ev["e"] == "TJOIN":
        c = ev["child"]
        out.append((c, len(g.threads[c]["events"]) - 1))
    return out


# ---------------------------------------------------------------- layout shared by tikz and dot

TEX_TEXT = {"\\": r"\textbackslash{}", "{": r"\{", "}": r"\}", "$": r"\$", "&": r"\&",
            "#": r"\#", "^": r"\textasciicircum{}", "_": r"\_", "%": r"\%",
            "~": r"\textasciitilde{}"}


def tex(s):
    return "".join(TEX_TEXT.get(c, c) for c in s)


def kw(tag):
    if re.fullmatch(r"[A-Za-z0-9_]+", tag):
        return r"\kw{%s}" % tag.replace("_", r"\_")
    return r"\texttt{%s}" % tex(tag)


def is_record(v):
    return isinstance(v, dict) and list(v) == ["t", "f"]


def label_tex(v, mode):
    if is_opaque(v):
        return r"\texttt{?}"
    if is_record(v):
        if mode == "tag":
            subs = [label_tex(f, mode) for f in v["f"] if is_record(f)]
        else:
            subs = [label_tex(f, mode) for f in v["f"]]
        return kw(v["t"]) + ("(%s)" % ",".join(subs) if subs else "")
    if isinstance(v, bool):
        return r"\mathit{true}" if v else r"\mathit{false}"
    if is_int(v):
        return str(v)
    if isinstance(v, str):
        return r"\texttt{%s}" % tex(v)
    if isinstance(v, list):
        return "[%s]" % ",".join(label_tex(x, mode) for x in v)
    return r"\texttt{?}"


def label_plain(v, mode):
    if is_opaque(v):
        return "?"
    if is_record(v):
        if mode == "tag":
            subs = [label_plain(f, mode) for f in v["f"] if is_record(f)]
        else:
            subs = [label_plain(f, mode) for f in v["f"]]
        return v["t"] + ("(%s)" % ", ".join(subs) if subs else "")
    if isinstance(v, bool):
        return "true" if v else "false"
    if is_int(v):
        return str(v)
    if isinstance(v, str):
        return json_str(v)
    if isinstance(v, list):
        return "[%s]" % ", ".join(label_plain(x, mode) for x in v)
    return "?"


def fmt_num(x):
    s = ("%.4f" % x).rstrip("0").rstrip(".")
    return "0" if s in ("-0", "") else s


class Layout:
    def __init__(self, g, opts):
        self.g = g
        threads = g.threads
        names = [th["name"] for th in threads]
        shown_kinds = {"SEND", "RECV", "BLK", "ERROR"}
        if opts.show_nondet:
            shown_kinds.add("NONDET")
        self.shown = {}
        for t, th in enumerate(threads):
            if t == 0 and not opts.show_main:
                continue
            evs = [k for k, ev in enumerate(th["events"]) if ev["e"] in shown_kinds]
            if evs:
                self.shown[t] = evs
        vis = [names.index(x) for x in g.visible]
        cols = [t for t in vis if t in self.shown]
        cols += [t for t in range(len(threads)) if t in self.shown and t not in vis]
        self.cols = cols
        self.col_of = {t: j for j, t in enumerate(cols)}

        def base(t):
            n = names[t]
            return (n[0] if n else "t").upper()

        self.letter = {}
        self.node_prefix = {}
        by_letter = {}
        for t in cols:
            by_letter.setdefault(base(t), []).append(t)
        for L, ts in by_letter.items():
            low = L.lower() if re.fullmatch(r"[a-z0-9]", L.lower()) else "q"
            for r, t in enumerate(ts, 1):
                if len(ts) == 1:
                    self.letter[t] = (L, None)
                    self.node_prefix[t] = low
                else:
                    self.letter[t] = (L, r)
                    self.node_prefix[t] = "%s%dx" % (low, r)
        for t in range(len(threads)):
            if t not in self.letter:
                self.letter[t] = (base(t), None)
        self.node = {}
        for t, evs in self.shown.items():
            for i, k in enumerate(evs, 1):
                self.node[(t, k)] = "%s%d" % (self.node_prefix[t], i)
        # rows, in porf order
        self.row = {}
        last = {}
        for e in g.order:
            if e not in self.node:
                continue
            t, k = e
            r = last[t] + 1 if t in last else 0
            ev = threads[t]["events"][k]
            if ev["e"] == "RECV" and ev["rf"] is not None and tuple(ev["rf"]) in self.node:
                r = max(r, self.row[tuple(ev["rf"])])
            self.row[e] = r
            last[t] = r
        # message edges between shown events, by the receive's row, then column
        rfs = []
        for (t, k), n in self.node.items():
            ev = threads[t]["events"][k]
            if ev["e"] == "RECV" and ev["rf"] is not None and tuple(ev["rf"]) in self.node:
                rfs.append((self.row[(t, k)], self.col_of[t], self.node[tuple(ev["rf"])], n))
        rfs.sort()
        self.rf_edges = [(a, b) for _, _, a, b in rfs]
        self.unread = [(t, k) for (t, k) in self.node
                       if threads[t]["events"][k]["e"] == "SEND" and (t, k) not in g.read_by]

    def letter_tex(self, t):
        L, r = self.letter[t]
        return L if r is None else "%s_%d" % (L, r)

    def letter_plain(self, t):
        L, r = self.letter[t]
        return L if r is None else "%s_%d" % (L, r)

    def name(self, t):
        n = self.g.threads[t]["name"]
        return n if n is not None else "#%d" % t


def badge_default(g):
    return "%s %s, %s" % (g.export["program"], g.export["size"], g.sha[:8])


# ---------------------------------------------------------------- tikz

STANDALONE_HEAD = r"""\documentclass[border=4pt]{standalone}
\usepackage{amsmath,amssymb}
\usepackage{tikz}
\usetikzlibrary{arrows.meta,positioning,fit,backgrounds,calc}
% from plan/case-studies/a2a/report/macros.tex 159-160, 335, 359-377
\newcommand{\sendop}{\ensuremath{\mathsf{send}}}
\newcommand{\recvop}{\ensuremath{\mathsf{recv}}}
\newcommand{\kw}[1]{\ensuremath{\mathsf{#1}}}
\tikzset{
  ev/.style     = {inner sep=2pt},
  gbox/.style   = {draw=blue!45!black, rounded corners=2pt,
  inner xsep=7pt, inner ysep=6pt},
  gbadge/.style = {fill=blue!45!black, text=white, rounded corners=2pt,
  inner sep=1.5pt, font=\tiny\bfseries, anchor=north west},
  rfe/.style    = {->, dashed, green!60!black},
  poe/.style    = {->, darkgray},
  obox/.style   = {draw=blue!45!black, fill=blue!5, rounded corners=2pt,
                   inner xsep=7pt, inner ysep=6pt},
  obadge/.style = {fill=blue!45!black, text=white, rounded corners=2pt,
                   inner sep=1.5pt, font=\tiny\bfseries, anchor=north west},
  ibox/.style   = {draw=orange!75!black, fill=yellow!16, rounded corners=2pt,
                   inner xsep=7pt, inner ysep=6pt},
  ibadge/.style = {fill=orange!75!black, text=white, rounded corners=2pt,
                   inner sep=1.5pt, font=\tiny\bfseries, anchor=north west},
  hlink/.style  = {densely dotted, gray!60!black},
  obsl/.style   = {font=\scriptsize, inner sep=1pt},
}
\begin{document}
"""


def event_tex(g, lay, t, k, mode):
    ev = g.threads[t]["events"][k]
    e = ev["e"]
    if e == "SEND":
        return r"$\sendop(%s,%s)$" % (lay.letter_tex(ev["to"]), label_tex(ev["v"], mode))
    if e == "RECV":
        return r"$\recvop()$" if ev["rf"] is not None else r"$\recvop()=\bot$"
    if e == "BLK":
        return r"$\kw{blocked}$"
    if e == "ERROR":
        return r"$\kw{error}$"
    if e == "NONDET":
        return r"$\mathsf{nd}{=}%d$" % ev["i"]
    raise AssertionError(e)


# ---------------------------------------------------------------- fitting --width
#
# Character widths in pt, measured once with pdflatex in the A2A report's preamble (acmart acmsmall,
# Libertine text, newtx math): \scriptsize is 7 pt, \tiny 6 pt. Classes: sf = \mathsf (tags, send,
# recv), mit = \mathit (booleans), it = math italic (thread letters), tt = \texttt (strings), ti =
# \textit (column headers), rm = math symbols (a comma or `=` with its math spacing), sub = one
# subscript digit, sp = the space in a header. A character missing from a class counts as the
# widest of that class. Every estimate is multiplied by WIDTH_MARGIN.
CHAR_PT = {
    'scriptsize': {
        'sf': ('0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ_abcdefghijklmnopqrstuvwxyz', [3.394, 3.394, 3.394, 3.394, 3.394, 3.394, 3.394, 3.394, 3.394, 3.394, 4.592, 4.497, 4.942, 5.183, 3.927, 3.679, 5.234, 5.3, 2.146, 2.336, 4.592, 3.759, 6.468, 5.241, 5.409, 4.081, 5.913, 4.38, 3.694, 3.942, 5.052, 4.606, 6.942, 4.241, 4.249, 4.541, 3.402, 3.511, 3.679, 3.132, 3.869, 3.307, 2.657, 3.643, 4.008, 1.898, 1.986, 3.672, 1.84, 5.891, 3.935, 3.65, 3.818, 3.811, 2.591, 2.891, 2.482, 3.92, 3.373, 5.271, 3.321, 3.694, 3.037]),
        'mit': ('0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz', [3.532, 3.241, 3.544, 3.42, 3.442, 3.595, 3.442, 3.957, 3.502, 3.611, 4.869, 4.106, 4.887, 5.07, 4.041, 4.037, 5.15, 5.674, 2.827, 3.621, 5.271, 3.789, 6.38, 5.752, 5.157, 4.19, 5.157, 4.19, 3.595, 4.789, 5.548, 5.296, 7.596, 5.344, 4.841, 4.913, 3.588, 3.592, 3.092, 3.989, 3.095, 3.818, 3.763, 3.854, 2.164, 2.208, 3.8, 2.27, 5.738, 3.796, 3.391, 3.719, 3.697, 3.132, 2.756, 2.661, 3.916, 3.789, 5.365, 3.909, 4.008, 3.409]),
        'it': ('0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz', [3.394, 3.394, 3.394, 3.394, 3.394, 3.394, 3.394, 3.394, 3.394, 3.394, 5.022, 4.665, 4.99, 5.749, 4.482, 4.53, 5.515, 5.96, 2.967, 3.752, 5.522, 4.187, 6.742, 6.344, 5.617, 4.566, 5.654, 4.573, 4.084, 4.723, 5.796, 5.373, 7.482, 5.727, 5.008, 5.187, 3.92, 3.895, 3.351, 4.15, 3.508, 4.468, 3.442, 4.055, 2.464, 3.405, 4.07, 2.493, 6.114, 4.154, 3.829, 4.23, 3.942, 3.384, 3.157, 3.048, 4.179, 3.595, 5.866, 4.23, 4.508, 3.548]),
        'tt': (' !"#$%&\'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_`abcdefghijklmnopqrstuvwxyz{|}~', [3.5, 3.5, 3.5, 3.5, 3.5, 3.5, 3.5, 3.5, 3.5, 3.5, 3.5, 3.5, 3.5, 3.5, 3.5, 3.5, 3.5, 3.5, 3.5, 3.5, 3.5, 3.5, 3.5, 3.5, 3.5, 3.5, 3.5, 3.5, 3.5, 3.5, 3.5, 3.5, 3.5, 3.5, 3.5, 3.5, 3.5, 3.5, 3.5, 3.5, 3.5, 3.5, 3.5, 3.5, 3.5, 3.5, 3.5, 3.5, 3.5, 3.5, 3.5, 3.5, 3.5, 3.5, 3.5, 3.5, 3.5, 3.5, 3.5, 3.5, 3.5, 3.5, 3.5, 3.5, 3.5, 3.5, 3.5, 3.5, 3.5, 3.5, 3.5, 3.5, 3.5, 3.5, 3.5, 3.5, 3.5, 3.5, 3.5, 3.5, 3.5, 3.5, 3.5, 3.5, 3.5, 3.5, 3.5, 3.5, 3.5, 3.5, 3.5, 3.5, 3.5, 3.5, 3.5]),
        'ti': (' !"#$%&\'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_`abcdefghijklmnopqrstuvwxyz{|}~', [1.75, 2.079, 2.677, 3.37, 3.202, 4.459, 5.177, 2.21, 2.81, 2.338, 3.223, 3.689, 1.533, 2.439, 1.533, 2.817, 3.384, 3.108, 3.398, 3.283, 3.3, 3.454, 3.3, 3.791, 3.358, 3.462, 1.641, 1.627, 3.573, 3.689, 3.409, 3.398, 5.925, 4.669, 3.937, 4.69, 4.861, 3.874, 3.867, 4.938, 5.44, 2.708, 3.472, 5.05, 3.633, 6.118, 5.516, 4.938, 4.017, 4.938, 4.021, 3.454, 4.588, 5.32, 5.078, 7.284, 5.125, 4.644, 4.714, 2.971, 2.247, 2.572, 3.626, 3.402, 2.53, 3.44, 3.44, 2.957, 3.829, 2.971, 3.661, 3.615, 3.699, 2.079, 2.117, 3.636, 2.176, 5.498, 3.643, 3.251, 3.559, 3.545, 3.003, 2.635, 2.544, 3.755, 3.629, 5.141, 3.748, 3.846, 3.269, 2.593, 1.677, 1.95, 3.335]),
        'rm': ('(),=[]⊥', [3.38, 3.38, 2.812, 8.377, 3.307, 3.307, 5.198]),
        'sub': 2.338, 'sp': 1.712},
    'tiny': {
        'sf': ('0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ_abcdefghijklmnopqrstuvwxyz', [3.022, 3.022, 3.022, 3.022, 3.022, 3.022, 3.022, 3.022, 3.022, 3.022, 4.088, 4.004, 4.401, 4.615, 3.497, 3.276, 4.66, 4.719, 1.911, 2.08, 4.088, 3.347, 5.759, 4.667, 4.816, 3.634, 5.265, 3.9, 3.289, 3.51, 4.498, 4.101, 6.181, 3.776, 3.783, 4.043, 2.916, 3.127, 3.276, 2.788, 3.445, 2.944, 2.366, 3.243, 3.568, 1.69, 1.768, 3.269, 1.638, 5.245, 3.503, 3.25, 3.399, 3.393, 2.308, 2.574, 2.21, 3.49, 3.003, 4.693, 2.957, 3.289, 2.704]),
        'mit': ('0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz', [3.145, 2.886, 3.156, 3.045, 3.065, 3.201, 3.065, 3.523, 3.118, 3.215, 4.335, 3.656, 4.352, 4.514, 3.598, 3.594, 4.586, 5.052, 2.517, 3.224, 4.693, 3.373, 5.681, 5.122, 4.592, 3.731, 4.592, 3.731, 3.201, 4.264, 4.94, 4.716, 6.763, 4.758, 4.31, 4.374, 3.195, 3.198, 2.753, 3.552, 2.756, 3.399, 3.351, 3.432, 1.927, 1.966, 3.383, 2.021, 5.109, 3.38, 3.019, 3.312, 3.292, 2.788, 2.454, 2.369, 3.487, 3.373, 4.777, 3.481, 3.568, 3.035]),
        'it': ('0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz', [3.022, 3.022, 3.022, 3.022, 3.022, 3.022, 3.022, 3.022, 3.022, 3.022, 4.472, 4.153, 4.443, 5.119, 3.991, 4.033, 4.911, 5.307, 2.642, 3.341, 4.917, 3.728, 6.003, 5.648, 5.002, 4.066, 5.034, 4.072, 3.637, 4.205, 5.161, 4.784, 6.662, 5.099, 4.459, 4.618, 3.49, 3.468, 2.983, 3.695, 3.123, 3.978, 3.065, 3.611, 2.194, 3.032, 3.624, 2.22, 5.444, 3.699, 3.409, 3.767, 3.51, 3.013, 2.811, 2.714, 3.721, 3.201, 5.223, 3.767, 4.014, 3.159]),
        'tt': (' !"#$%&\'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_`abcdefghijklmnopqrstuvwxyz{|}~', [3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0]),
        'ti': (' !"#$%&\'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_`abcdefghijklmnopqrstuvwxyz{|}~', [1.5, 1.782, 2.295, 2.889, 2.745, 3.822, 4.438, 1.894, 2.409, 2.004, 2.763, 3.162, 1.314, 2.091, 1.314, 2.415, 2.9, 2.664, 2.913, 2.814, 2.829, 2.961, 2.829, 3.249, 2.878, 2.968, 1.407, 1.395, 3.063, 3.162, 2.922, 2.913, 5.079, 4.002, 3.375, 4.02, 4.167, 3.321, 3.315, 4.233, 4.663, 2.321, 2.976, 4.329, 3.114, 5.244, 4.728, 4.233, 3.443, 4.233, 3.447, 2.96, 3.933, 4.56, 4.353, 6.243, 4.393, 3.981, 4.041, 2.547, 1.926, 2.205, 3.108, 2.916, 2.169, 2.949, 2.949, 2.535, 3.282, 2.547, 3.138, 3.098, 3.171, 1.782, 1.815, 3.117, 1.865, 4.713, 3.123, 2.787, 3.051, 3.039, 2.574, 2.259, 2.181, 3.219, 3.111, 4.407, 3.213, 3.297, 2.802, 2.223, 1.438, 1.671, 2.859]),
        'rm': ('(),=[]⊥', [3.009, 3.009, 2.504, 7.459, 2.944, 2.944, 4.628]),
        'sub': 2.417, 'sp': 1.467},
}

WIDTH_MARGIN = 1.10
PT_CM = 2.54 / 72.27
EV_PAD_PT = 4.0             # ev: inner xsep 2pt on both sides
GBOX_PAD_PT = 14.0          # gbox: inner xsep 7pt on both sides
COL_GAP_CM = 0.15           # least white space between two labels of one row
UNREAD_TAG_PT = 22.0        # `\tiny unread` with its node's inner sep
_TAB = {}


def _cls(font, cls):
    key = (font, cls)
    if key not in _TAB:
        chars, ws = CHAR_PT[font][cls]
        _TAB[key] = (dict(zip(chars, ws)), max(ws))
    return _TAB[key]


def pt(font, cls, text):
    d, mx = _cls(font, cls)
    return sum(d.get(c, mx) for c in text)


def kw_pt(font, tag):
    if re.fullmatch(r"[A-Za-z0-9_]+", tag):
        return pt(font, "sf", tag)
    return pt(font, "tt", tag)


def value_pt(font, v, mode):
    """Width in pt of label_tex(v, mode)."""
    if is_opaque(v):
        return pt(font, "tt", "?")
    if is_record(v):
        if mode == "tag":
            subs = [value_pt(font, f, mode) for f in v["f"] if is_record(f)]
        else:
            subs = [value_pt(font, f, mode) for f in v["f"]]
        w = kw_pt(font, v["t"])
        if subs:
            w += pt(font, "rm", "()") + sum(subs) + pt(font, "rm", ",") * (len(subs) - 1)
        return w
    if isinstance(v, bool):
        return pt(font, "mit", "true" if v else "false")
    if is_int(v):
        return pt(font, "sf", str(abs(v))) + (pt(font, "rm", "=") if v < 0 else 0)
    if isinstance(v, str):
        return pt(font, "tt", v)
    if isinstance(v, list):
        return (pt(font, "rm", "[]") + sum(value_pt(font, x, mode) for x in v)
                + pt(font, "rm", ",") * max(0, len(v) - 1))
    return pt(font, "tt", "?")


def letter_pt(font, lay, t):
    L, r = lay.letter[t]
    return pt(font, "it", L) + (CHAR_PT[font]["sub"] * len(str(r)) if r is not None else 0)


def event_pt(g, lay, t, k, mode, font, broken=False):
    """Estimated width in pt of an event's node, margin and padding included."""
    ev = g.threads[t]["events"][k]
    e = ev["e"]
    if e == "SEND":
        head = pt(font, "sf", "send") + pt(font, "rm", "(,") + letter_pt(font, lay, ev["to"])
        tail = value_pt(font, ev["v"], mode) + pt(font, "rm", ")")
        w = max(head, tail) if broken else head + tail
    elif e == "RECV":
        w = pt(font, "sf", "recv") + pt(font, "rm", "()")
        if ev["rf"] is None:
            w += pt(font, "rm", "=⊥")
    elif e == "BLK":
        w = pt(font, "sf", "blocked")
    elif e == "ERROR":
        w = pt(font, "sf", "error")
    else:
        w = pt(font, "sf", "nd%d" % ev["i"]) + pt(font, "rm", "=")
    return w * WIDTH_MARGIN + EV_PAD_PT


def header_pt(lay, t, font):
    return (pt(font, "ti", lay.name(t)) + CHAR_PT[font]["sp"] + letter_pt(font, lay, t)) \
        * WIDTH_MARGIN + EV_PAD_PT


def _span(items, x, sep):
    """(left, right) in cm of items (column index, width cm) at x unit x."""
    lo = min(j * sep * x - w / 2 for j, w in items)
    hi = max(j * sep * x + w / 2 for j, w in items)
    return lo, hi


def _width_at(panels_items, unread_items, badge_cm, x, sep):
    """Picture width in cm at x unit x: the widest panel, its frame, unread arrows, the badge."""
    best = 0.0
    for items, heads, unread in zip(panels_items, unread_items[0], unread_items[1]):
        lo, hi = _span(items, x, sep)
        pad = GBOX_PAD_PT * PT_CM / 2
        lo, hi = lo - pad, hi + pad
        hi = max(hi, lo + badge_cm)
        for j, w, left in unread:
            ext = w / 2 + 1.2 * x + UNREAD_TAG_PT * PT_CM
            if left:
                lo = min(lo, j * sep * x - ext)
            else:
                hi = max(hi, j * sep * x + ext)
        hlo, hhi = _span(heads, x, sep)
        best = max(best, max(hi, hhi) - min(lo, hlo))
    return best


def _layout_try(g, lay, opts, panels, font, broken):
    """(x unit or None, picture width at that x) for one font and set of broken labels."""
    sep = opts.col_sep
    wid = {e: event_pt(g, lay, e[0], e[1], opts.label, font, e in broken) * PT_CM for e in lay.node}
    panels_items, heads_all, unread_all = [], [], []
    xmin = 0.0
    unread = set(lay.unread)
    for p in panels:
        jj = {t: j for j, t in enumerate(p)}
        items = [(jj[t], wid[(t, k)]) for t in p for k in lay.shown[t]]
        heads = [(jj[t], header_pt(lay, t, font) * PT_CM) for t in p]
        un = [(jj[t], wid[(t, k)], jj[t] == 0 and len(p) > 1)
              for t in p for k in lay.shown[t] if (t, k) in unread]
        panels_items.append(items)
        heads_all.append(heads)
        unread_all.append(un)
        rows = {}
        for t in p:
            for k in lay.shown[t]:
                rows.setdefault(lay.row[(t, k)], []).append((jj[t], wid[(t, k)]))
        rows["head"] = heads
        for r in rows.values():
            r.sort()
            for (ja, wa), (jb, wb) in zip(r, r[1:]):
                if jb > ja:
                    xmin = max(xmin, ((wa + wb) / 2 + COL_GAP_CM) / ((jb - ja) * sep))
    title = opts.title if opts.title is not None else badge_default(g)
    badge_cm = (pt("tiny", "ti", "G: " + title) * 1.15 * WIDTH_MARGIN + 3) * PT_CM
    W = lambda x: _width_at(panels_items, (heads_all, unread_all), badge_cm, x, sep)
    if W(1.0) <= opts.width:
        x = 1.0
    elif W(0.0) > opts.width:
        return None, W(0.0), xmin
    else:
        lo, hi = 0.0, 1.0
        for _ in range(40):
            mid = (lo + hi) / 2
            if W(mid) <= opts.width:
                lo = mid
            else:
                hi = mid
        x = int(lo * 1000) / 1000.0
    if x < xmin:
        return None, W(x), xmin
    return x, W(x), xmin


def tikz_geometry(g, lay, opts):
    """Font, x unit, panels and broken labels so that the figure fits --width.

    The x unit is the largest at most 1 at which the widest panel (labels, frame, unread arrows,
    headers) fits and no two labels of a row overlap. Failing that, the font drops to \tiny; then
    the widest send labels are broken after their destination, one at a time."""
    ncols = len(lay.cols)
    cpr = max(1, opts.cols_per_row)
    panels = [lay.cols[i:i + cpr] for i in range(0, ncols, cpr)] if ncols > cpr else [lay.cols]
    fonts = ["scriptsize", "tiny"] if ncols <= 4 else ["tiny"]
    for font in fonts:
        x, w, _ = _layout_try(g, lay, opts, panels, font, set())
        if x is not None:
            return font, x, panels, set()
    font = "tiny"
    sends = sorted((e for e in lay.node if g.threads[e[0]]["events"][e[1]]["e"] == "SEND"),
                   key=lambda e: -event_pt(g, lay, e[0], e[1], opts.label, font))
    broken = set()
    for e in sends:
        broken.add(e)
        x, w, xmin = _layout_try(g, lay, opts, panels, font, broken)
        if x is not None:
            return font, x, panels, broken
    x, w, xmin = _layout_try(g, lay, opts, panels, font, broken)
    print("witness-render: %s: does not fit %.1f cm even in \\tiny with broken labels; "
          "labels of a row overlap. Use a smaller --cols-per-row or a larger --width"
          % (g.sha[:8], opts.width), file=sys.stderr)
    return font, max(0.05, min(1.0, xmin)), panels, broken


def broken_tex(g, lay, t, k, mode):
    ev = g.threads[t]["events"][k]
    return r"$\sendop(%s,$\\$%s)$" % (lay.letter_tex(ev["to"]), label_tex(ev["v"], mode))


def render_tikz(g, opts):
    lay = Layout(g, opts)
    sep = opts.col_sep
    font, xu, panels, broken = tikz_geometry(g, lay, opts)
    pos = {}       # column -> (x index within its panel, y offset of its panel in rows)
    offset = 0
    for p in panels:
        for j, t in enumerate(p):
            pos[t] = (j, offset)
        last = max((lay.row[(t, k)] for t in p for k in lay.shown[t]), default=0)
        offset += last + 5
    out = []
    if opts.standalone:
        out.append(STANDALONE_HEAD)
    out.append("%% witness-render %d: %s\n" % (VERSION, g.sha))
    out.append("\\begin{tikzpicture}[>=Stealth, font=\\%s, x=%scm, y=0.45cm, "
               "ev/.append style={inner ysep=1pt}]\n" % (font, fmt_num(xu)))
    for t in lay.cols:
        j, off = pos[t]
        out.append("  \\node[font=\\%s\\itshape] at (%s,%s) {%s $%s$};\n"
                   % (font, fmt_num(j * sep), fmt_num(1.8 - off), tex(lay.name(t)),
                      lay.letter_tex(t)))
    for t in lay.cols:
        j, off = pos[t]
        for k in lay.shown[t]:
            if (t, k) in broken:
                style, body = "ev, align=center, inner ysep=0pt", broken_tex(g, lay, t, k, opts.label)
            else:
                style, body = "ev", event_tex(g, lay, t, k, opts.label)
            out.append("  \\node[%s] (%s) at (%s,%s) {%s};\n"
                       % (style, lay.node[(t, k)], fmt_num(j * sep),
                          fmt_num(-lay.row[(t, k)] - off), body))
    po = []
    for t in lay.cols:
        ns = [lay.node[(t, k)] for k in lay.shown[t]]
        po += ["%s/%s" % (a, b) for a, b in zip(ns, ns[1:])]
    if po:
        out.append("  \\foreach \\a/\\b in {%s} \\draw[poe] (\\a) -- (\\b);\n" % ",".join(po))
    if lay.rf_edges:
        out.append("  \\foreach \\a/\\b in {%s} \\draw[rfe] (\\a) -- (\\b);\n"
                   % ",".join("%s/%s" % e for e in lay.rf_edges))
    panel_len = {t: len(p) for p in panels for t in p}
    for (t, k) in sorted(lay.unread, key=lambda e: (lay.col_of[e[0]], e[1])):
        j = pos[t][0]
        left = j == 0 and panel_len[t] > 1
        out.append("  \\draw[rfe] (%s) -- ++(%s,0) node[%s]{\\tiny unread};\n"
                   % (lay.node[(t, k)], "-1.2" if left else "1.2", "left" if left else "right"))
    fit = "".join("(%s)" % lay.node[(t, k)] for t in lay.cols for k in lay.shown[t])
    if fit:
        out.append("  \\node[gbox, fit=%s] (bx) {};\n" % fit)
        title = opts.title if opts.title is not None else tex(badge_default(g))
        out.append("  \\node[gbadge] at (bx.north west) {$G$: %s};\n" % title)
    out.append("\\end{tikzpicture}\n")
    if opts.standalone:
        out.append("\\end{document}\n")
    return "".join(out)


# ---------------------------------------------------------------- dot

def dq(s):
    return '"%s"' % s.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")


def event_plain(g, lay, t, k, mode):
    ev = g.threads[t]["events"][k]
    e = ev["e"]
    if e == "SEND":
        return "send(%s, %s)" % (lay.letter_plain(ev["to"]), label_plain(ev["v"], mode))
    if e == "RECV":
        return "recv()" if ev["rf"] is not None else "recv() = \u22a5"
    if e == "BLK":
        return "blocked"
    if e == "ERROR":
        return "error"
    if e == "NONDET":
        return "nd=%d" % ev["i"]
    raise AssertionError(e)


def render_dot(g, opts):
    lay = Layout(g, opts)
    h = g.sha[:8]
    title = opts.title if opts.title is not None else "%s %s  %s" % (
        g.export["program"], g.export["size"], h)
    unread = set(lay.unread)
    out = ["digraph %s {\n" % dq(h),
           "  graph [rankdir=TB, newrank=true, nodesep=0.5, ranksep=0.2, labelloc=t, label=%s];\n"
           % dq(title),
           '  node [shape=plaintext, fontname="Helvetica", fontsize=10];\n',
           "  edge [arrowsize=0.6];\n"]
    for t in lay.cols:
        items = []
        for k in lay.shown[t]:
            extra = ', xlabel="unread"' if (t, k) in unread else ""
            items.append("%s [label=%s%s];" % (lay.node[(t, k)],
                                               dq(event_plain(g, lay, t, k, opts.label)), extra))
        out.append("  subgraph cluster_%d { label=%s; %s }\n"
                   % (t, dq("%s (%s)" % (lay.name(t), lay.letter_plain(t))), " ".join(items)))
    out.append("  edge [color=gray30];\n")
    for t in lay.cols:
        ks = lay.shown[t]
        parts = []
        for a, b in zip(ks, ks[1:]):
            d = lay.row[(t, b)] - lay.row[(t, a)]
            parts.append("%s -> %s%s;" % (lay.node[(t, a)], lay.node[(t, b)],
                                          " [minlen=%d]" % d if d != 1 else ""))
        if parts:
            out.append("  %s\n" % " ".join(parts))
    if lay.rf_edges:
        out.append('  edge [style=dashed, color="#2e7d32", constraint=false];\n')
        out.append("  %s\n" % " ".join("%s -> %s;" % e for e in lay.rf_edges))
    rows = {}
    for t in lay.cols:
        for k in lay.shown[t]:
            rows.setdefault(lay.row[(t, k)], []).append(lay.node[(t, k)])
    groups = ["{rank=same; %s}" % " ".join("%s;" % n for n in ns)
              for _, ns in sorted(rows.items()) if len(ns) > 1]
    for i in range(0, len(groups), 5):
        out.append("  %s\n" % " ".join(groups[i:i + 5]))
    out.append("}\n")
    return "".join(out)


# ---------------------------------------------------------------- text (P5 witness)

def render_text(g, opts):
    threads = g.threads
    it = g.run["iteration"]
    out = ["uncovered summary (iteration %s): %s\n" % ("-" if it is None else it, g.line)]
    for t, th in enumerate(threads):
        if th["name"] is None:
            raise Refused("thread %d is unnamed; the text witness refuses it" % t)
    for t, th in enumerate(threads):
        out.append('thread "%s"[tid=%d%s]:\n' % (th["name"], t, ", daemon" if th["daemon"] else ""))
        for k, ev in enumerate(th["events"]):
            e = ev["e"]
            if e == "TCREATE":
                s = 'TCREATE(%d:"%s")' % (ev["child"], threads[ev["child"]]["name"])
            elif e == "SEND":
                s = "SEND(%s, %s)" % (threads[ev["to"]]["name"], dumps(ev["v"]))
            elif e == "RECV":
                if ev["rf"] is None:
                    s = "RECV() [TIMEOUT]"
                else:
                    ts, ks = ev["rf"]
                    s = "RECV() [(%d, %d) %s]" % (ts, ks, threads[ts]["name"])
            elif e == "NONDET":
                s = "NONDET %d" % ev["i"]
            elif e == "TJOIN":
                s = "TJOIN(%d)" % ev["child"]
            elif e == "ERROR":
                s = "ERROR(%s)" % ev["msg"]
            else:
                s = e
            out.append("\t(%d, %d): %s\n" % (t, k, s))
    return "".join(out)


def render_summary(g):
    names = [th["name"] for th in g.threads]
    vis = [names.index(x) for x in g.visible]
    order = vis + [t for t in range(len(g.threads)) if t not in vis]
    out = []
    for t in order:
        w, st = word_of(g, t)
        out.append("%s\t%s\t%s\t%s\n" % (names[t] if names[t] is not None else "#%d" % t,
                                         "visible" if t in vis else "hidden", st, dumps(w)))
    return "".join(out)


# ---------------------------------------------------------------- driver

EXT = {"tikz": ".tex", "dot": ".dot", "text": ".witness.txt"}
RENDER = {"tikz": render_tikz, "dot": render_dot, "text": render_text}


def write_new(path, content):
    data = content.encode("utf-8")
    if os.path.exists(path):
        with open(path, "rb") as f:
            if f.read() == data:
                return
        raise Refused("%s exists and differs; not overwritten" % path)
    tmp = path + ".tmp-%d" % os.getpid()
    with open(tmp, "wb") as f:
        f.write(data)
    os.replace(tmp, path)


def read_lines(path):
    if path == "-":
        data = sys.stdin.buffer.read()
    else:
        with open(path, "rb") as f:
            data = f.read()
    text = data.decode("utf-8")
    if not text.endswith("\n"):
        raise Invalid(1, "the file does not end with a newline")
    return text[:-1].split("\n")


def inputs(paths):
    for p in paths:
        if p != "-" and os.path.isdir(p):
            for n in sorted(os.listdir(p)):
                if n.endswith(".graph.json"):
                    yield os.path.join(p, n)
        else:
            yield p


def stem_of(path, store_index):
    base = "stdin" if path == "-" else os.path.basename(path)
    if store_index is None:
        for suf in (".graph.json", ".json", ".jsonl"):
            if base.endswith(suf):
                return base[:-len(suf)]
        return base
    if base.endswith(".jsonl"):
        base = base[:-len(".jsonl")]
    return "%s.%d" % (base, store_index)


def graphs_of(path, lines, select):
    """Yield (where, store_index, obj or Invalid, raw); checks the store rules S1-S4."""
    first = None
    try:
        first = parse_line(lines[0])
    except ValueError:
        pass
    if isinstance(first, dict) and "witness_store" in first:
        where0 = "%s:1" % path
        try:
            keys_exact(first, STORE_KEYS, "the store header")
            if first["witness_store"] != 1:
                fail(1, "witness_store is not 1")
            if dumps(first) != lines[0]:
                fail(1, "the store header is not in byte form")
            check_export(first["export"])
            check_run(first["run"])
            if first["run"]["iteration"] is not None:
                fail(1, "the store header's run.iteration is not null")
        except Invalid as e:
            yield where0, None, e, None
            return
        seen = {}
        for i, raw in enumerate(lines[1:], 1):
            if select and i not in select:
                continue
            where = "%s:%d" % (path, i + 1)
            try:
                obj = parse_line(raw)
            except ValueError as e:
                yield where, i, Invalid(1, "not JSON: %s" % e), raw
                continue
            if isinstance(obj, dict):
                if obj.get("export") != first["export"]:
                    yield where, i, Invalid("S1", "export differs from the store header's"), raw
                    continue
                run = obj.get("run")
                if isinstance(run, dict) and {k: v for k, v in run.items() if k != "iteration"} != {
                        k: v for k, v in first["run"].items() if k != "iteration"}:
                    yield where, i, Invalid("S3", "run differs from the store header's "
                                            "(iteration aside)"), raw
                    continue
                if obj.get("join") is not None or obj.get("nearest") is not None:
                    yield where, i, Invalid("S2", "join or nearest is not null in a store"), raw
                    continue
                ln = obj.get("line")
                if isinstance(ln, str):
                    if ln in seen:
                        yield where, i, Invalid("S4", "line repeats the graph at store line %d"
                                                % seen[ln]), raw
                        continue
                    seen[ln] = i + 1
            yield where, i, obj, raw
        return
    if len(lines) != 1:
        yield path, None, Invalid(1, "%d lines; a graph file holds one, a store starts with "
                                     "witness_store" % len(lines)), None
        return
    try:
        obj = parse_line(lines[0])
    except ValueError as e:
        yield path, None, Invalid(1, "not JSON: %s" % e), None
        return
    yield path, None, obj, lines[0]


def main(argv=None):
    ap = argparse.ArgumentParser(
        description="Check and render witness graphs (format 1).",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="exit codes:\n"
               "  0  every graph valid (and every output written)\n"
               "  1  --check mismatch: some graph's recomputed summary line pi(G) differs from its\n"
               "     stored line (G9); every other rule held\n"
               "  2  invalid graph (any of G1-G8, G10-G12, store rules S1-S4), unreadable input,\n"
               "     refused output, or bad usage; 2 wins over 1")
    ap.add_argument("files", nargs="+", metavar="FILE", help="graph file, store, directory or -")
    ap.add_argument("--check", action="store_true", help="validate G1-G12 only (the default)")
    ap.add_argument("--format", default="", help="tikz, dot, text, comma-separated")
    ap.add_argument("--tikz", action="store_true")
    ap.add_argument("--dot", action="store_true")
    ap.add_argument("--text", action="store_true")
    ap.add_argument("--summary", action="store_true", help="print every thread's word")
    ap.add_argument("--out-dir")
    ap.add_argument("--index", default="", help="graphs of a store to use, 1-based, comma-separated")
    ap.add_argument("--show-nondet", action="store_true")
    ap.add_argument("--show-main", action="store_true")
    ap.add_argument("--label", choices=("tag", "full"), default="tag")
    ap.add_argument("--col-sep", type=float, default=4.8)
    ap.add_argument("--title")
    ap.add_argument("--standalone", action="store_true")
    ap.add_argument("--width", type=float, default=16.5,
                    help="tikz: width in cm to fit (\\textwidth); the x unit shrinks to fit (default 16.5)")
    ap.add_argument("--cols-per-row", type=int, default=5,
                    help="tikz: thread columns per row of panels; more columns wrap (default 5)")
    ap.add_argument("-q", "--quiet", action="store_true", help="no per-graph ok lines")
    opts = ap.parse_args(argv)

    formats = [f for f in opts.format.split(",") if f]
    for f in ("tikz", "dot", "text"):
        if getattr(opts, f) and f not in formats:
            formats.append(f)
    for f in formats:
        if f not in EXT:
            ap.error("unknown format %s" % f)
    try:
        select = {int(x) for x in opts.index.split(",") if x}
    except ValueError:
        ap.error("--index takes 1-based integers")
    if opts.out_dir and not os.path.isdir(opts.out_dir):
        ap.error("--out-dir %s is not a directory" % opts.out_dir)

    checked = bad = mismatched = written = 0
    status = 0
    for path in inputs(opts.files):
        try:
            lines = read_lines(path)
        except (OSError, UnicodeDecodeError) as e:
            print("%s: %s" % (path, e), file=sys.stderr)
            status = 2
            continue
        except Invalid as e:
            print("%s: G%s: %s" % (path, e.rule, e.msg), file=sys.stderr)
            bad += 1
            status = 2
            continue
        for where, idx, obj, raw in graphs_of(path, lines, select):
            checked += 1
            try:
                if isinstance(obj, Invalid):
                    raise obj
                g = validate(obj, raw)
            except Invalid as e:
                r = e.rule if isinstance(e.rule, str) else "G%d" % e.rule
                print("%s: %s: %s" % (where, r, e.msg), file=sys.stderr)
                if e.rule == 9:
                    mismatched += 1
                    status = max(status, 1)
                else:
                    bad += 1
                    status = 2
                continue
            if opts.summary:
                sys.stdout.write("# %s %s\n" % (where, g.sha[:8]))
                sys.stdout.write(render_summary(g))
            if not formats:
                if not opts.quiet and not opts.summary:
                    print("%s: ok %s" % (where, g.sha[:8]))
                continue
            d = opts.out_dir or (os.path.dirname(path) if path != "-" else ".") or "."
            stem = stem_of(path, idx)
            for f in formats:
                out = os.path.join(d, stem + EXT[f])
                try:
                    write_new(out, RENDER[f](g, opts))
                    written += 1
                    if not opts.quiet:
                        print(out)
                except (Refused, OSError) as e:
                    print("%s: %s" % (where, e), file=sys.stderr)
                    status = 2
    print("witness-render: %d graph(s) checked, %d mismatched (G9), %d invalid%s"
          % (checked, mismatched, bad, ", %d file(s) written" % written if formats else ""),
          file=sys.stderr)
    return status


if __name__ == "__main__":
    sys.exit(main())
