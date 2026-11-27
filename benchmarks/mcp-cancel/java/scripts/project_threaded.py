#!/usr/bin/env python3
"""Projection of a threaded mcp-cancel export onto the wire vocabulary, rev 2.

Question: which export would the threaded Java models (bench/mcp_cancel/McpCancelThreaded.java,
McpCancelThreadedFixed.java) have if the relay events of their visible `server` process were not observations?
JMC writes every send and receive of a visible role's thread into its word (SummaryExporter); the models' `server`
process also exchanges relay messages (Java-only tags beginning with `J`) with the hidden SDK threads. This script
removes them so that the result can be joined against the specification like any other mcp-cancel export.

Input
  IN, an export (`.jsonl`, daavar/format/SPEC.md §2-§3) written by JMC for one of the two threaded models:
  - line 1, the header: compact JSON, keys in the writers' order, no duplicate or unknown key, `format` and `encoding`
    the integer 1, `minor` the integer 1 exactly when `roles` is present, `visible` (and `roles`) ["client","server"],
    `tool` "jmc", `model` "p2p", `enumeration` "complete", no `seed`/`iterations`, the online fields all or none. It
    is copied to the output unchanged.
  - every other line, one summary in normal form (json.dumps with separators (',', ':') gives the same bytes, after
    removing a trailing compact integer `n` member): keys `statuses`, `words`, `counts`; roles exactly `client`,
    `server`; a status `done`, `blocked` or `err`; an event `{"s"|"r":{"t":TAG,"f":[…]}}`. Accepted events: the
    client's and the server's msgs.rs vocabulary (VOCAB below, with its field types; a `Call{r,t}` has t = `t<r>`,
    and each r is called at most once in each role's word), and, in the server's word only, the relay events RELAY
    below with their directions and field types. Anything else, an empty line, a repeated summary (compared without
    `n`), or counts of the wrong length or range, refuses the file. Lines end in LF or CRLF (SPEC §7.1).
  - `n`, the multiplicity, beyond the format (which makes it optional per line and a signed 64-bit integer): either
    every summary line has it or none, and it is in 0..2^63-1, because merged lines carry the sum of their `n`; a
    sum that leaves that range refuses the file. JMC writes `n` on every line.

Rule (per summary line)
  Let the server word be s_1..s_m and K the indices of its kept events (the VOCAB events; the RELAY events are
  dropped). The client word is kept as it is.
  - Counts of a server event (against the client word): unchanged for every kept event; the entries of dropped
    events are removed. A server event's count is the number of client events porf-before it; dropping server events
    changes no such set.
  - Counts of a client event (against the server word): c becomes |{k in K : k <= c}|, the number of kept events
    among the first c events of the original server word. The server is one sequential thread, so the server events
    porf-before a client event form a prefix s_1..s_c of its word, and the kept ones among them are exactly the kept
    events of that prefix.
  - Statuses unchanged.
  Then lines that have become equal are merged: two projected lines with equal statuses and words and equal counts
  become one line, with `n` the sum of their `n` (absent when the input has no `n`). Two projected lines with equal
  statuses and words and different counts are not merged: both are written, and the case is reported (a
  `conflict`). The output lines are sorted by their bytes, as the writers sort them.

Output
  OUT, created new (an existing file is not overwritten): the header, then the projected lines.
  stdout: `<IN> lines=N relay_events=R out_lines=M merged=G conflicts=C`.
  --json REPORT: optional, created new; the same counts and, per conflict group (projected lines with equal statuses
  and words and different counts), its output lines as written.
  --self-test: run only the built-in vectors and mutants (no file read).
  Exit code: 0 if no conflict, 1 if some conflict (the output is still written), 2 if the input is refused, an
  output exists, or a self-test fails. The self-tests run before any file is read.
"""
import json, os, re, sys

class Refused(Exception):
    pass

REQ, TOK, VAL, CODE = 'req', 'tok', 'val', 'code'
ROLES = ['client', 'server']
STATUSES = ('done', 'blocked', 'err')

# msgs.rs, as plan/case-studies/mcp/fix/scripts/joinfmt.py VOCAB: (role, direction, tag) -> field kinds.
VOCAB = {
    ('server', 'r', 'Call'): (REQ, TOK), ('server', 'r', 'Cancelled'): (REQ,), ('server', 'r', 'Prog'): (REQ, TOK, VAL),
    ('server', 'r', 'Done'): (REQ,), ('server', 'r', 'Shutdown'): (),
    ('server', 's', 'Progress'): (TOK, VAL), ('server', 's', 'Result'): (REQ,), ('server', 's', 'Error'): (REQ, CODE),
    ('server', 's', 'Work'): (REQ, TOK), ('server', 's', 'Stop'): (REQ,), ('server', 's', 'Shutdown'): (),
    ('client', 's', 'Call'): (REQ, TOK), ('client', 's', 'Cancelled'): (REQ,), ('client', 's', 'Shutdown'): (),
    ('client', 'r', 'Progress'): (TOK, VAL), ('client', 'r', 'Result'): (REQ,), ('client', 'r', 'Error'): (REQ, CODE),
}
# The relay events of the server process (McpCancelThreaded*.java, the J... records it sends or receives).
RELAY = {
    ('s', 'JLineCall'): (REQ, TOK), ('s', 'JLineCancelled'): (REQ,), ('r', 'JRead'): (),
    ('r', 'JNext'): (REQ,), ('s', 'JProg'): (REQ, TOK, VAL), ('s', 'JDone'): (REQ,),
    ('r', 'JWork'): (REQ, TOK), ('r', 'JStop'): (REQ,), ('s', 'JStopped'): (),
    ('r', 'JWriteProgress'): (TOK, VAL), ('r', 'JWriteResult'): (REQ,), ('s', 'JWritten'): (),
}

HDR_KEYS = ['format', 'minor', 'tool', 'program', 'size', 'visible', 'roles', 'model', 'enumeration', 'seed',
            'iterations', 'first_miss_iteration', 'first_miss_wall_ms', 'uncovered_total', 'encoding']
HDR_REQ = ('format', 'tool', 'program', 'size', 'visible', 'model', 'enumeration', 'encoding')
N_TAIL = re.compile(r'(\{.*),"n":(0|-?[1-9][0-9]*)\}')

def _no_dup(pairs):
    keys = [k for k, _ in pairs]
    if len(set(keys)) != len(keys):
        raise Refused(f'duplicate key in {keys}')
    return dict(pairs)

def _field_ok(kind, x):
    if kind == REQ:
        return type(x) is int and x in (1, 2)
    if kind == TOK:
        return type(x) is str and x != ''
    if kind == VAL:
        return type(x) is int and 1 <= x <= 2**32 - 1
    return x == '*'

def dumps(o):
    return json.dumps(o, separators=(',', ':'), ensure_ascii=False)

def header(text):
    try:
        h = json.loads(text, object_pairs_hook=_no_dup)
    except json.JSONDecodeError as e:
        raise Refused(f'header: {e}')
    if not isinstance(h, dict) or dumps(h) != text:
        raise Refused('header not compact JSON')
    ks = list(h)
    if any(k not in HDR_KEYS for k in ks) or ks != [k for k in HDR_KEYS if k in h] or any(k not in h for k in HDR_REQ):
        raise Refused(f'header keys {ks}')
    one = lambda x: type(x) is int and x == 1
    if not one(h['format']) or not one(h['encoding']) or ('minor' in h) != ('roles' in h) or not one(h.get('minor', 1)):
        raise Refused('header format/encoding/minor/roles')
    if any(type(h[k]) is not str for k in ('tool', 'program', 'size', 'model', 'enumeration')):
        raise Refused('header tool/program/size/model/enumeration not all strings')
    if 'seed' in h or 'iterations' in h:
        raise Refused('header seed/iterations in a complete export')
    onl = [k for k in ('first_miss_iteration', 'first_miss_wall_ms', 'uncovered_total') if k in h]
    if onl and (len(onl) != 3 or any(type(h[k]) is not int for k in onl)):
        raise Refused(f'header online fields {onl}')
    if h['visible'] != ROLES or h.get('roles', ROLES) != ROLES:
        raise Refused(f'header visible {h["visible"]}, roles {h.get("roles")}')
    if h['tool'] != 'jmc' or h['model'] != 'p2p' or h['enumeration'] != 'complete':
        raise Refused(f'header tool {h["tool"]}, model {h["model"]}, enumeration {h["enumeration"]}')

def summary(line):
    """Validate one summary line; return (statuses, words, counts, n or None)."""
    m = N_TAIL.fullmatch(line)
    text, n = (m.group(1) + '}', int(m.group(2))) if m else (line, None)
    if n is not None and not 0 <= n < 2**63:
        raise Refused('n outside 0..2^63-1')
    try:
        o = json.loads(text, object_pairs_hook=_no_dup)
    except json.JSONDecodeError as e:
        raise Refused(f'not JSON: {e}')
    if dumps(o) != text:
        raise Refused('not in normal form')
    if not isinstance(o, dict) or list(o) != ['statuses', 'words', 'counts']:
        raise Refused('keys are not statuses, words, counts')
    st, ws, cs = o['statuses'], o['words'], o['counts']
    if not isinstance(st, dict) or list(st) != ROLES or any(v not in STATUSES for v in st.values()):
        raise Refused(f'bad statuses {st!r}')
    if not isinstance(ws, dict) or list(ws) != ROLES or any(not isinstance(w, list) for w in ws.values()):
        raise Refused('bad words object')
    for role in ROLES:
        for e in ws[role]:
            if not isinstance(e, dict) or list(e) not in (['s'], ['r']):
                raise Refused(f'{role}: event {dumps(e)} is not exactly one of s, r')
            d = next(iter(e)); v = e[d]
            if not isinstance(v, dict) or list(v) != ['t', 'f'] or type(v['t']) is not str or not isinstance(v['f'], list):
                raise Refused(f'{role}: event {dumps(e)} is not {{"t":TAG,"f":[...]}}')
            kinds = VOCAB.get((role, d, v['t']))
            if kinds is None and role == 'server':
                kinds = RELAY.get((d, v['t']))
            if kinds is None:
                raise Refused(f'{role}: {d} {v["t"]} is outside the vocabulary')
            if len(v['f']) != len(kinds) or not all(_field_ok(k, x) for k, x in zip(kinds, v['f'])):
                raise Refused(f'{role}: event {dumps(e)} has bad arity or field types')
            if v['t'] == 'Call' and v['f'][1] != f"t{v['f'][0]}":
                raise Refused(f'{role}: Call {v["f"]} with a token other than t{v["f"][0]}')
        calls = [v['f'][0] for e in ws[role] for d, v in e.items() if v['t'] == 'Call']
        if len(calls) != len(set(calls)):
            raise Refused(f'{role}: a request is called twice')
    nc, ns = len(ws['client']), len(ws['server'])
    if not isinstance(cs, list) or len(cs) != nc + ns or not all(type(c) is int and c >= 0 for c in cs) \
            or any(c > ns for c in cs[:nc]) or any(c > nc for c in cs[nc:]):
        raise Refused('bad counts')
    return st, ws, cs, n, text

def is_relay(e):
    d = next(iter(e))
    return (d, e[d]['t']) in RELAY

def project(st, ws, cs):
    """The rule of the header; returns (statuses, words, counts, number of dropped events)."""
    nc = len(ws['client'])
    server = ws['server']
    keep = [not is_relay(e) for e in server]
    kept_before = [0]                      # kept_before[c] = kept events among the first c server events
    for k in keep:
        kept_before.append(kept_before[-1] + k)
    client_counts = [kept_before[c] for c in cs[:nc]]
    server_counts = [c for c, k in zip(cs[nc:], keep) if k]
    words = {'client': ws['client'], 'server': [e for e, k in zip(server, keep) if k]}
    return st, words, client_counts + server_counts, keep.count(False)

def run(lines):
    """Project the lines of an export; return (output lines, report). Raises Refused on any deviation."""
    L = [l[:-2] if l.endswith('\r\n') else l[:-1] if l.endswith('\n') else l for l in lines]
    if not L:
        raise Refused('line 1: empty file')
    try:
        header(L[0])
    except Refused as e:
        raise Refused(f'line 1: {e}')
    seen = set(); groups = {}; relay = 0; has_n = None
    for i in range(1, len(L)):
        try:
            st, ws, cs, n, text = summary(L[i])
        except Refused as e:
            raise Refused(f'line {i + 1}: {e}')
        if text in seen:
            raise Refused(f'line {i + 1}: repeated summary (n removed)')
        seen.add(text)
        if has_n is None:
            has_n = n is not None
        elif has_n != (n is not None):
            raise Refused(f'line {i + 1}: n present on some lines only')
        st, ws, cs, r = project(st, ws, cs)
        relay += r
        key = dumps({'statuses': st, 'words': ws})
        groups.setdefault(key, {}).setdefault(tuple(cs), []).append(n)
    out = []; merged = 0; conflicts = []
    for key, by_counts in groups.items():
        o = json.loads(key)
        group = []
        for cs, ns in by_counts.items():
            merged += len(ns) - 1
            text = dumps({'statuses': o['statuses'], 'words': o['words'], 'counts': list(cs)})
            if has_n and sum(ns) >= 2**63:
                raise Refused(f'merged n {sum(ns)} outside 0..2^63-1 for {text}')
            group.append(text[:-1] + f',"n":{sum(ns)}}}' if has_n else text)
        if len(group) > 1:
            conflicts.append(sorted(group, key=lambda s: s.encode()))
        out += group
    out.sort(key=lambda s: s.encode())
    rep = {'lines': len(L) - 1, 'relay_events': relay, 'out_lines': len(out), 'merged': merged,
           'conflicts': len(conflicts), 'conflict_groups': conflicts}
    return [L[0]] + out, rep

# ---------------------------------------------------------------- self-test

HDR = ('{"format":1,"minor":1,"tool":"jmc","program":"mcp-cancel","size":"aa","visible":["client","server"],'
       '"roles":["client","server"],"model":"p2p","enumeration":"complete","encoding":1}')

def ev(spec):
    """'!Call{1,t1}' -> {"s":{"t":"Call","f":[1,"t1"]}}"""
    d = 's' if spec[0] == '!' else 'r'
    tag, rest = spec[1:].split('{', 1)
    f = [int(x) if re.fullmatch(r'[0-9]+', x) else x for x in rest[:-1].split(',') if x != '']
    return {d: {'t': tag, 'f': f}}

def line(client, server, counts, n=None, statuses=('done', 'done')):
    o = {'statuses': dict(zip(ROLES, statuses)), 'words': {'client': [ev(x) for x in client.split()],
         'server': [ev(x) for x in server.split()]}, 'counts': counts}
    t = dumps(o)
    return t if n is None else t[:-1] + f',"n":{n}}}'

# A threaded word: the server's word with relay events, and its projection computed by hand.
C1 = '!Call{1,t1} ?Result{1} !Shutdown{}'
S1 = '?JRead{} ?Call{1,t1} !JLineCall{1,t1} ?JWork{1,t1} !Work{1,t1} ?JNext{1} ?Done{1} !JDone{1} ?JWriteResult{1} !Result{1} !JWritten{} ?JRead{} ?Shutdown{} !Shutdown{} !Shutdown{}'
#  server index:      1         2            3               4            5          6        7       8           9               10        11         12         13          14         15
# client counts (server events porf-before): !Call: 0; ?Result{1}: 10 (through !Result{1}); !Shutdown: 10
# server counts (client events porf-before): JRead 0, Call 1, JLineCall 1, JWork 1, Work 1, JNext 1, Done 1, JDone 1,
#   JWriteResult 1, Result 1, JWritten 1, JRead 1, Shutdown 3, Shutdown 3, Shutdown 3
IN1 = line(C1, S1, [0, 10, 10] + [0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 3, 3, 3], n=5)
# kept: Call(2) Work(5) Done(7) Result(10) Shutdown(13,14,15): kept among first 10 = 4
OUT1 = line(C1, '?Call{1,t1} !Work{1,t1} ?Done{1} !Result{1} ?Shutdown{} !Shutdown{} !Shutdown{}',
            [0, 4, 4] + [1, 1, 1, 1, 3, 3, 3], n=5)
# The same projection from a different relay interleaving (JNext before JWork): merged, n summed.
S1b = '?JRead{} ?Call{1,t1} !JLineCall{1,t1} ?JNext{1} ?JWork{1,t1} !Work{1,t1} ?Done{1} !JDone{1} ?JWriteResult{1} !Result{1} !JWritten{} ?JRead{} ?Shutdown{} !Shutdown{} !Shutdown{}'
IN1b = line(C1, S1b, [0, 10, 10] + [0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 3, 3, 3], n=2)
# The same word with a different causal count: a client event that saw fewer server events -> a conflict.
C2 = '!Call{1,t1} !Cancelled{1} ?Result{1} !Shutdown{}'
S2 = '?JRead{} ?Call{1,t1} !JLineCall{1,t1} ?JWork{1,t1} !Work{1,t1} ?JRead{} ?Cancelled{1} !JLineCancelled{1} ?Done{1} ?JWriteResult{1} !Result{1} !JWritten{} ?JRead{} ?Shutdown{} !Shutdown{} !Shutdown{}'
#  indices: 1 JRead, 2 Call, 3 JLineCall, 4 JWork, 5 Work, 6 JRead, 7 Cancelled, 8 JLineCancelled, 9 Done, 10 JWriteResult, 11 Result, ...
IN2a = line(C2, S2, [0, 0, 11, 11] + [0, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2, 2, 4, 4, 4], n=1)
IN2b = line(C2, S2, [0, 5, 11, 11] + [0, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2, 2, 4, 4, 4], n=1)

C2P = '?Call{1,t1} !Work{1,t1} ?Cancelled{1} ?Done{1} !Result{1} ?Shutdown{} !Shutdown{} !Shutdown{}'
OUT2a = line(C2, C2P, [0, 0, 5, 5] + [1, 1, 2, 2, 2, 4, 4, 4], n=1)
OUT2b = line(C2, C2P, [0, 2, 5, 5] + [1, 1, 2, 2, 2, 4, 4, 4], n=1)

VECTORS = [
    ('relay dropped, counts remapped', [HDR, IN1], [HDR, OUT1], 0, 0),
    ('two relay orders merge, n summed', [HDR, IN1, IN1b], [HDR, OUT1[:-2] + '7}'], 1, 0),
    ('equal words, different counts: conflict, both kept', [HDR, IN2a, IN2b], [HDR, OUT2a, OUT2b], 0, 1),
    ('CRLF line ends', [HDR + '\r\n', IN1 + '\r\n'], [HDR, OUT1], 0, 0),
    ('no n on any line', [HDR, line(C1, S1, [0, 10, 10] + [0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 3, 3, 3])],
     [HDR, line(C1, '?Call{1,t1} !Work{1,t1} ?Done{1} !Result{1} ?Shutdown{} !Shutdown{} !Shutdown{}',
                [0, 4, 4] + [1, 1, 1, 1, 3, 3, 3])], 0, 0),
]

def _mutants():
    sub = lambda old, new: IN1.replace(old, new, 1)
    return [
        ('empty file', []),
        ('header tool traceforge', [HDR.replace('"jmc"', '"traceforge"'), IN1]),
        ('header with space', [HDR.replace(',"size"', ', "size"'), IN1]),
        ('header model asyn', [HDR.replace('"p2p"', '"asyn"'), IN1]),
        ('header sampled', [HDR.replace('"complete"', '"sampled"'), IN1]),
        ('header minor without roles', [HDR.replace(',"roles":["client","server"]', ''), IN1]),
        ('header duplicate key', [HDR.replace('"size":"aa"', '"size":"aa","size":"aa"'), IN1]),
        ('empty line', [HDR, '']),
        ('repeated line', [HDR, IN1, IN1]),
        ('relay tag in client word', [HDR, line('!Call{1,t1} ?JRead{} !Shutdown{}', S1, [0, 0, 10] + [0] * 15, n=1)]),
        ('relay with wrong direction', [HDR, sub('{"s":{"t":"JLineCall"', '{"r":{"t":"JLineCall"')]),
        ('relay with wrong arity', [HDR, sub('{"t":"JNext","f":[1]}', '{"t":"JNext","f":[1,2]}')]),
        ('unknown tag', [HDR, sub('{"t":"JNext","f":[1]}', '{"t":"JNope","f":[1]}')]),
        ('visible event bad field', [HDR, sub('{"t":"Done","f":[1]}', '{"t":"Done","f":[3]}')]),
        ('boolean request id', [HDR, sub('{"t":"Done","f":[1]}', '{"t":"Done","f":[true]}')]),
        ('not normal form', [HDR, IN1.replace('"counts":', '"counts": ', 1)]),
        ('duplicate key', [HDR, IN1.replace('"statuses":{', '"statuses":{"client":"done",', 1)]),
        ('bad status', [HDR, IN1.replace('"server":"done"', '"server":"stuck"', 1)]),
        ('counts too short', [HDR, IN1.replace('"counts":[0,10,', '"counts":[10,', 1)]),
        ('client count above server length', [HDR, IN1.replace('"counts":[0,10,', '"counts":[0,16,', 1)]),
        ('negative n', [HDR, IN1.replace('"n":5}', '"n":-5}')]),
        ('n on some lines only', [HDR, IN1, IN1b.replace(',"n":2}', '}')]),
        ('null event', [HDR, sub('{"r":{"t":"JNext","f":[1]}}', '{"r":null}')]),
        ('repeated summary, different n', [HDR, IN1, IN1.replace('"n":5}', '"n":6}')]),
        ('merged n beyond 2^63-1', [HDR, IN1.replace('"n":5}', f'"n":{2**63 - 1}}}'), IN1b.replace('"n":2}', '"n":1}')]),
        ('Call token not t<r>', [HDR, IN1.replace('{"s":{"t":"Call","f":[1,"t1"]}}', '{"s":{"t":"Call","f":[1,"t2"]}}', 1)]),
        ('request called twice', [HDR, line('!Call{1,t1} !Call{1,t1} !Shutdown{}', S1, [0, 0, 10] + [0] * 15, n=1)]),
    ]

def selftest():
    fails = []
    for name, inp, want, merged, conflicts in VECTORS:
        try:
            out, rep = run(inp)
        except Refused as e:
            fails.append(f'vector {name}: refused: {e}'); continue
        if want is not None and out != want:
            fails.append(f'vector {name}: output {out[1:]} != {want[1:]}')
        if rep['merged'] != merged or rep['conflicts'] != conflicts:
            fails.append(f'vector {name}: merged {rep["merged"]}, conflicts {rep["conflicts"]}')
        if conflicts and len(out) != 3:
            fails.append(f'vector {name}: a conflict must keep both lines')
    for name, inp in _mutants():
        try:
            run(inp)
            fails.append(f'mutant {name}: accepted')
        except Refused:
            pass
    return fails

def main(argv):
    fails = selftest()
    if fails:
        print('self-test failed:\n  ' + '\n  '.join(fails)); return 2
    if argv == ['--self-test']:
        print(f'self-test passed: {len(VECTORS)} vectors, {len(_mutants())} mutants'); return 0
    js = None
    if '--json' in argv:
        i = argv.index('--json')
        if i + 1 >= len(argv):
            print('usage: project_threaded.py IN OUT [--json REPORT] | --self-test'); return 2
        js = argv[i + 1]; argv = argv[:i] + argv[i + 2:]
    if len(argv) != 2 or any(a.startswith('--') for a in argv):
        print('usage: project_threaded.py IN OUT [--json REPORT] | --self-test'); return 2
    src, dst = argv
    for p in [dst] + ([js] if js else []):
        if os.path.exists(p):
            print(f'{p} exists; not overwritten'); return 2
    try:
        with open(src, encoding='utf-8', newline='') as f:
            out, rep = run(f.readlines())
    except (Refused, UnicodeDecodeError) as e:
        print(f'{src}: refused: {e}'); return 2
    with open(dst, 'x', encoding='utf-8', newline='') as f:
        f.write(''.join(l + '\n' for l in out))
    if js:
        with open(js, 'x', encoding='utf-8') as f:
            json.dump({src: rep}, f, indent=1)
    print(f'{src} lines={rep["lines"]} relay_events={rep["relay_events"]} out_lines={rep["out_lines"]} '
          f'merged={rep["merged"]} conflicts={rep["conflicts"]}')
    return 1 if rep['conflicts'] else 0

if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
