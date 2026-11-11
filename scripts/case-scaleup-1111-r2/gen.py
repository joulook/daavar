#!/usr/bin/env python3
"""case-scaleup-1111 round 2 step 2 (task of 2026-11-11 about 01:55; brief plan/eval2/case-scaleup-1111/BRIEF.md). The helper section
below is copied verbatim from ../case-scaleup-1111/gen.py (scratch dir changed to tmp/case-scaleup-1111-r2/, first-round rows added
to LIVE as pending). New: steps +1..+3 on every size axis of every maximal measured size of a join family, classified as
Part A (the size's id sits only in rows files: no record, not live; or failed for a reason that does not repeat),
Part B (new size, TraceForge pair) or Part C (new size, a Java side: XJ1/XJ2/XC1/XC2/XT* joins and XCE/XJE exports),
'later' (an export of the pair at a size <= C is running/pending without an ok record at that size, or proposed in this round)
and 'infeasible' (an export at a size <= C hit a limit, or the estimate exceeds the budget; decided in emit.py).
Reads $SCR/load.pkl (load.py); writes $SCR/cands.pkl. Estimates are planning numbers only (R10)."""
import collections, pickle, re, sys
G = '/local/home/mkhoshechin/Documents/github-jmc/'; SCR = '/local/home/mkhoshechin/tmp/case-scaleup-1111-r2/'
D = pickle.load(open(SCR + 'load.pkl', 'rb'))
R = collections.defaultdict(list)
for r in D['recs']: R[r['run_id']].append(r)
ROWS, SIDE, LIVE = D['rows'], D['side'], D['live']
# r2: the first round's rows (cluster/campaign/case-scaleup-1111/*-pool.rows) are being queued: treat them as pending
for _i, _v in ROWS.items():
    if any(x[0].startswith('case-scaleup-1111/') and not x[0].startswith('case-scaleup-1111/r2/') for x in _v) and _i not in LIVE: LIVE[_i] = ('pending-r1', 'case-scaleup-1111', '')
SUF = re.compile(r'^(t\d+|w\d+|c48h|c1h|cinf|j\d|m|r\d+|p|x\d+|f\d+|g|failed-\d+)$')
LIMIT = re.compile(r'timeout|memory|heap|killed|oom|jmc cap', re.I)
def parse(i):
    p = i.split('.'); toks = p[2].split('_')
    return p[0], p[1], toks, p[3:]
def nums(i): return tuple(int(t) for t in parse(i)[2] if t.isdigit())
def fkey(i):   # family key: prefix, dir, words (in order), non-suffix tail tags
    pre, d, toks, tail = parse(i)
    return (pre, d, '_'.join(t for t in toks if not t.isdigit()), '.'.join(t for t in tail if not SUF.match(t)))
def ekey(i):   # export key: family key plus the thread tag
    pre, d, toks, tail = parse(i); tt = [t for t in tail if re.match(r'^[tw]\d+$', t)]
    return fkey(i) + (tt[0] if tt else '',)
def base2(i):
    pre, d, toks, tail = parse(i)
    return (pre, d, tuple(sorted(t for t in toks if not t.isdigit())), tuple(t for t in toks if t.isdigit()), tuple(sorted(t for t in tail if not SUF.match(t))))
ALL = set(ROWS) | set(R) | set(LIVE)
B2 = collections.defaultdict(set)
for i in ALL: B2[base2(i)].add(i)
def recok(i):
    v = [r for r in R.get(i, []) if r['stop_reason'] == 'ok']; return v[-1] if v else None
def f(x):
    try: return float(x)
    except: return None
def node_factor(n): return 0.5 if n.startswith(('volta', 'pinky')) else 1.0   # time measured on volta/pinky -> brain-equivalent
# ---- export database: ekey -> nums -> list of (brain-eq seconds, MB, id, node) for ok runs; failures at a limit; live
EX = collections.defaultdict(lambda: collections.defaultdict(list)); EXF = collections.defaultdict(list); EXL = collections.defaultdict(list)
EXRE = re.compile(r'^(C3E|XCE|XJE)\.')
for i in ALL:
    if not EXRE.match(i): continue
    k, n = ekey(i), nums(i)
    for r in R.get(i, []):
        if r['stop_reason'] == 'ok' and f(r['wall_ms']):
            EX[k][n].append((f(r['wall_ms']) / 1000 * node_factor(r['node']), (f(r['peak_rss_kb']) or 0) / 1024, i, r['node']))
        elif LIMIT.search(r['stop_reason']): EXF[k].append((n, i, r['stop_reason']))
    if i in LIVE: EXL[k].append((n, i, LIVE[i][0]))
def ex_best(k, n):
    v = EX[k].get(n, []); return max(v) if v else None
# ---- program domains (BRIEF: check every size against the program's domain)
def axes_of(d):
    if d.startswith(('raft-rs', 'omnipaxos')): return (1, 1, 0)        # grid-axes.tsv: third argument structural
    if d.startswith('zookeeper'): return (1, 0)                          # <e> <late>: late is a 0/1 switch (INTERFACE.md)
    return None
def valid(d, c):
    if d.startswith('crdb'):
        n, p, m, a = c; return n in (1, 2) and 0 <= p <= n and m in (1, 2) and a >= 1 and not (n == 1 and p == 0)
    if d.startswith('scalardb'): return c[0] in (1, 2)                  # k in {1,2} (INTERFACE.md)
    return all(x >= 1 for x in c) if not d.startswith(('raft-rs', 'omnipaxos')) else True
LEFT = {}   # family -> reason
def leftout(d):
    if d.startswith('tikv'): return 'TiKV: sizes are scenario switches <scenario> <async> <onepc> <a>, not a ladder (scaleup-1110 assumption 1); left out'
    if d.startswith('scalardb'): return None
    return None
EXPLOR = {('raft-rs', 'reorder')}   # exploratory-variants.tsv
EXIDRE = re.compile(r'"\$EVAL_OUT"/(?:imports/)?([^/ ]+)/export\.jsonl')
def le(a, b): return len(a) == len(b) and all(x <= y for x, y in zip(a, b))
def sub_nums(i, c):
    pre, d, toks, tail = parse(i); it = iter(c)
    return pre, d, '_'.join(str(next(it)) if t.isdigit() else t for t in toks), tail
def fresh(idbase):
    i = idbase; r = 2
    while i in ALL or i in USED: i = idbase + f'.r{r}'; r += 1
    USED.add(i); return i
USED = set()
def new_export_id(tid, c):
    pre, d, desc, tail = sub_nums(tid, c); tt = [t for t in tail if re.match(r'^[tw]\d+$', t)]
    return f'{pre}.{d}.{desc}.' + ('.'.join(tt) + '.' if tt else '') + 'cinf'
def new_join_id(tid, c):
    pre, d, desc, tail = sub_nums(tid, c); keep = [t for t in tail if not SUF.match(t)]
    return f'{pre}.{d}.{desc}.' + ''.join(t + '.' for t in keep) + 'cinf.m'
def sub_cmd(cmd, c):
    cmd = re.sub(r'^env JAVA_TOOL_OPTIONS=\S+ ', '', cmd)
    if 'rust-run.sh' in cmd:
        head, args = cmd.split(' -- ', 1); toks = args.split()
        if sum(t.isdigit() for t in toks) != len(c): return None
        it = iter(c); args = ' '.join(str(next(it)) if t.isdigit() else t for t in toks)
        m = re.search(r'--size (\S+)', head)
        if m:
            st = m.group(1).split(','); it2 = iter(c)
            if sum(t.isdigit() for t in st) != len(c): return None
            head = head.replace(m.group(0), '--size ' + ','.join(str(next(it2)) if t.isdigit() else t for t in st))
        head = re.sub(r'(--threads \d+)', r'\1', head)
        return head + ' -- ' + args
    if 'jmc-run.sh' in cmd:
        m = re.search(r"'([^':]+):([^']*)'", cmd); st = m.group(2).split(',')
        if sum(t.isdigit() for t in st) != len(c): return None
        it = iter(c); cmd = cmd.replace(m.group(0), "'" + m.group(1) + ':' + ','.join(str(next(it)) if t.isdigit() else t for t in st) + "'")
        return re.sub(r'(export\.jsonl) (\d+)( |$)', lambda mm: mm.group(1) + ' 8640000' + mm.group(3), cmd, count=1)
    return None
def export_template(eid):
    """a rows command for this export's key: the id itself, else any id of the same export key"""
    if eid in ROWS: return eid, ROWS[eid][0][3]
    k = ekey(eid)
    for i in sorted(ROWS):
        if EXRE.match(i) and ekey(i) == k: return i, ROWS[i][0][3]
    return None, None
def step_ratio(k, P, ax):
    """time and memory ratio of export key k along axis ax: the step ending at P if measured, else the largest measured step;
    accelerating growth (the step before had a smaller ratio) multiplies g by the ratio of the two ratios"""
    steps = []
    for q in EX[k]:
        q2 = list(q)
        if len(q2) <= ax: continue
        q2[ax] += 1; q2 = tuple(q2)
        a, b = ex_best(k, q), ex_best(k, q2)
        if a and b: steps.append((q2 == P, sum(q2), q, a, b))
    if not steps: return None
    steps.sort(); _, _, q, a, b = steps[-1]
    g = max(b[0], 1.0) / max(a[0], 1.0); mr = max(b[1], 1.0) / max(a[1], 1.0)
    q0 = list(q); q0[ax] -= 1; q0 = tuple(q0)
    a0 = ex_best(k, q0) if q0[ax] >= 0 else None
    acc = 1.0
    if a0:
        g0 = max(a[0], 1.0) / max(a0[0], 1.0)
        if g0 < g and g0 > 0: acc = g / g0
    return max(g, 1.0) * acc, mr, (a[2], b[2], round(a[0]), round(b[0])), acc
# ======== round 2 ========
def fail_repeats(r):
    """True when a record's failure would repeat: a timeout at the 48 h limit (or more), exit 101/126 of the program, a deterministic
    refusal by the format/split check ('failed: ... must:' or 'refused'), or a skip caused by such a failure of an export it read."""
    s = r.get('stop_reason', ''); e = r.get('exit_code', '')
    tos = f(r.get('timeout_s')) or 0
    if s == 'timeout' and tos >= 172800: return f'timeout at {tos:.0f} s'
    if s in ('exit-101', 'exit-126') or (s.startswith('exit-') and e in ('101', '126')): return s
    if s.startswith('failed:') or s == 'refused': return s[:90]
    if s.startswith('skipped:'):
        m = re.match(r'skipped: (\S+) (.*)$', s)
        if m:
            up = m.group(1); why = m.group(2)
            for r2 in R.get(up, []):
                w = fail_repeats(r2)
                if w: return f'upstream {up}: {w}'
            if why.startswith(('exit-101', 'exit-126')): return f'upstream {up}: {why}'
    return None
JF = collections.defaultdict(lambda: collections.defaultdict(list))
for i in ALL:
    if i.endswith('.m') and not EXRE.match(i): JF[fkey(i)][nums(i)].append(i)
JAVA = ('XJ1', 'XJ2', 'XC1', 'XC2', 'XT1', 'XT2')
cands, later, infeas, notes, partA_found = [], [], [], collections.defaultdict(list), {}
PROP = {}   # (ekey, C) -> new size proposed in this round (not measured anywhere)
def study_of2(pre, d):
    if d == 'raft-rs' and pre[:2] in ('XJ', 'XT', 'XC'): return 'MicroRaft'
    for s, pat in (('OmniPaxos', 'omnipaxos'), ('raft-rs', 'raft-rs'), ('Cassandra', 'cassandra'), ('TiKV', 'tikv'), ('ZooKeeper', 'zookeeper'),
                   ('Pineapple', 'pineapple'), ('ScalarDB', 'scalardb'), ('CockroachDB', 'crdb'), ('ChainPaxos', 'chainpaxos'),
                   ('BookKeeper', 'bk'), ('3FS', 'craq'), ('SwiftPaxos', 'swiftpaxos')):
        if d.startswith(pat): return s
    return '?'
LEFT = {}
order = []
for fk in sorted(JF):
    pre, d, words, tail = fk; fam = '.'.join(x for x in fk if x)
    lo = leftout(d)
    if lo: LEFT[fam] = lo; continue
    if any(d == a and b in words.split('_') for a, b in EXPLOR): LEFT[fam] = 'exploratory variant (exploratory-variants.tsv)'; continue
    meas = {}
    for n, ids in JF[fk].items():
        for i in ids:
            r = recok(i)
            if r: meas[n] = (r, i)
    if not meas: LEFT[fam] = 'no measured size'; continue
    maxi = [P for P in meas if not any(Q != P and le(P, Q) for Q in meas)]
    ax = axes_of(d) or tuple(1 for _ in next(iter(meas)))
    for P in maxi:
        for k in range(len(P)):
            if not ax[k]: continue
            for s_ in (1, 2, 3): order.append((s_, fk, fam, P, k, meas, pre, d))
order.sort(key=lambda x: x[0])   # all +1 first, so that a +2 sees the +1 exports proposed in this round
seenC = set()
for s_, fk, fam, P, k, meas, pre, d in order:
    r, jid = meas[P]; allconf = all(v[0]['verdict'] == 'conforms' for v in meas.values())
    st = study_of2(pre, d); java = pre in JAVA
    C = tuple(x + (s_ if j == k else 0) for j, x in enumerate(P))
    tagC = ','.join(map(str, C))
    if (fk, C) in seenC: continue
    seenC.add((fk, C))
    if not valid(d, C): notes[fam].append(f'{tagC} outside the domain'); continue
    if any(Q != P and le(C, Q) for Q in meas): continue
    cmd = ROWS[jid][0][3] if jid in ROWS else None
    if not cmd: notes[fam].append(f'{jid}: no rows command'); continue
    exs = [e for e in EXIDRE.findall(cmd) if EXRE.match(e) and e.count(".") >= 2]
    if len(exs) != 2: notes[fam].append(f'{jid}: join reads {len(exs)} export ids'); continue
    jn = new_join_id(jid, C); hit = sorted(B2.get(base2(jn), ()))
    part = 'C' if (java or any(e.startswith(('XCE', 'XJE')) for e in exs)) else 'B'
    found = ''
    if hit:
        if any(recok(i) for i in hit): notes[fam].append(f'{tagC} measured under {[i for i in hit if recok(i)][0]}'); continue
        lv = [i for i in hit if i in LIVE]
        if lv: notes[fam].append(f'{tagC} {LIVE[lv[0]][0]} ({lv[0]})'); continue
        reps = [(i, fail_repeats(x)) for i in hit for x in R.get(i, []) if fail_repeats(x)]
        if reps: infeas.append(dict(fam=fam, C=C, P=P, why=f'record {reps[0][0]}: {reps[0][1]} (would repeat)', part='A', study=st)); continue
        fails = [(i, x['stop_reason'], x['node']) for i in hit for x in R.get(i, [])]
        found = (f'stale {hit[0]} (rows file {ROWS[hit[0]][0][0] if hit[0] in ROWS else "-"}); ' +
                 (f'records: ' + '; '.join(f'{i} {s} on {n}' for i, s, n in fails[:3]) + ' (not a repeating cause)' if fails else 'no brain-data record, not running, not pending'))
        part = 'A'
    elif s_ == 1:
        found = 'new +1 size (not proposed by round 1)'
    blk = None; lat = None; eplan = []; gmax = 1.0; mr_ = []
    for e in exs:
        ek = ekey(e)
        fl = [x for x in EXF[ek] if le(x[0], C) and not ex_best(ek, x[0])]
        fl += [(nums(i2), i2, fail_repeats(x2)) for i2 in ALL if EXRE.match(i2) and ekey(i2) == ek and le(nums(i2), C) and not ex_best(ek, nums(i2))
               for x2 in R.get(i2, []) if fail_repeats(x2)] if not fl else []
        if fl: blk = f'export {fl[0][1]} hit a limit ({fl[0][2]}) at {",".join(map(str, fl[0][0]))} <= {tagC}'; break
        lv = [x for x in EXL[ek] if le(x[0], C) and not ex_best(ek, x[0])]
        if lv: lat = f'{lv[0][2]} export {lv[0][1]} at {",".join(map(str, lv[0][0]))}'; break
        pp = [q for (k2, q), nid in PROP.items() if k2 == ek and le(q, C) and q != C]
        if pp: lat = f'export of {e.split(".")[1]} at {",".join(map(str, pp[0]))} proposed in this round ({PROP[(ek, pp[0])]})'; break
        tid, tcmd = export_template(e)
        if not tcmd: blk = f'no rows template for {e}'; break
        ncmd = sub_cmd(tcmd, C)
        if not ncmd: blk = f'cannot substitute size into {tid}'; break
        done = ex_best(ek, C)
        if done:
            t, m, basis, g, kind = done[0], done[1], f'measured at C: {done[2]} {done[0]:.0f}s {done[1]:.0f}MB on {done[3]}', 1.0, 're-export (done on another node)'
        else:
            sp = ex_best(ek, P)
            if not sp: blk = f'export {e} has no ok record at {",".join(map(str, P))}'; break
            sr = step_ratio(ek, P, k)
            if not sr:
                alt = [step_ratio(k2, P, k) for k2 in list(EX) if k2[1].split('-')[0] == ek[1].split('-')[0] and k2[0] == ek[0] and k2 != ek]
                alt = [a for a in alt if a]
                if alt: sr = max(alt)
            if not sr and sp[0] < 60:   # r2: a cheap export (< 60 s at P) without a step along this axis: the largest step ratio of the same export along any axis
                alt = [step_ratio(ek, P, k3) for k3 in range(len(P)) if k3 != k]
                alt = [a for a in alt if a]
                if alt: sr = max(alt)
            if not sr: blk = f'no measured step of {e} along axis {k + 1}'; break
            g, mr, stp, acc = sr; g = g ** s_; mr = mr ** s_
            t = max(sp[0], 1.0) * g
            jmc = e.startswith(('XCE', 'XJE'))
            m = sp[1] * (g ** 0.6 if (jmc and sp[1] < 500) else min(g, max(mr, 1.0)))
            basis = f'{sp[2]} {sp[0]:.0f}s {sp[1]:.0f}MB x g={g:.2f} (step {stp[0]} {stp[2]}s -> {stp[1]} {stp[3]}s' + (f', accel x{acc:.2f}' if acc > 1 else '') + f'{", ratio applied %d times" % s_ if s_ > 1 else ""})'
            kind = 'new size'
        gmax = max(gmax, g)
        eplan.append(dict(old=e, ek=ek, cmd=ncmd, t=t, m=m, basis=basis, kind=kind, jmc=e.startswith(('XCE', 'XJE')), tid=tid, C=C))
    if blk:
        infeas.append(dict(fam=fam, C=C, P=P, why=blk, part=part, study=st)) if 'limit' in blk else notes[fam].append(f'{tagC} blocked: {blk}')
        continue
    if lat: later.append(dict(fam=fam, C=C, P=P, why=lat, part=part, study=st, step=s_, jid=jid)); continue
    jt = (f(r['wall_ms']) or 1000) / 1000 * node_factor(r['node']) * gmax; jm = (f(r['peak_rss_kb']) or 0) / 1024 * gmax
    c = dict(fam=fam, study=st, fk=fk, jid=jid, P=P, C=C, jn=jn, jcmd=cmd, exs=exs, eplan=eplan, jt=jt, jm=jm, part=part, found=found,
             jbasis=f'{jid} {(f(r["wall_ms"]) or 0) / 1000:.0f}s on {r["node"]} x g={gmax:.2f}', allconf=allconf, step=s_,
             verdicts=sorted((n, v[0]['verdict'], v[1]) for n, v in meas.items()), axis=k, stale=hit)
    cands.append(c)
    for e in eplan:
        if e['kind'] == 'new size': PROP.setdefault((e['ek'], C), c['jn'])
pickle.dump(dict(cands=cands, later=later, infeas=infeas, left=LEFT, notes=dict(notes)), open(SCR + 'cands.pkl', 'wb'))
print(len(cands), 'candidates', collections.Counter((c['part'], c['step']) for c in cands), len(later), 'later', len(infeas), 'infeasible', len(LEFT), 'left')
