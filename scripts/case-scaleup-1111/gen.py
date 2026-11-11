#!/usr/bin/env python3
"""case-scaleup-1111 step 2 (plan/eval2/case-scaleup-1111/BRIEF.md): next sizes of every Offline merge join (*.m) of the 13 case
studies, cost estimates, priorities, rows files per host class. Reads $SCR/load.pkl (load.py). Writes $SCR/cands.pkl and, with
argument --write, plan/eval2/case-scaleup-1111/candidates.tsv and cluster/campaign/case-scaleup-1111/<class>-pool.rows(.tsv).
Method = plan/eval2/SCALEUP-2026-11-10.md (next size = +1 on one size axis of a maximal measured size; cost from the step ratio
of the same export along the same axis), simplified as stated in PLAN.md. Estimates are planning numbers only (R10)."""
import collections, pickle, re, sys
G = '/local/home/mkhoshechin/Documents/github-jmc/'; SCR = '/local/home/mkhoshechin/tmp/case-scaleup-1111/'
D = pickle.load(open(SCR + 'load.pkl', 'rb'))
R = collections.defaultdict(list)
for r in D['recs']: R[r['run_id']].append(r)
ROWS, SIDE, LIVE = D['rows'], D['side'], D['live']
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
# ---- join families
JF = collections.defaultdict(lambda: collections.defaultdict(list))
for i in ALL:
    if i.endswith('.m') and not EXRE.match(i): JF[fkey(i)][nums(i)].append(i)
cands, groups, notes = [], {}, collections.defaultdict(list)
def study_of(d):
    for s, pat in (('MicroRaft', None), ('OmniPaxos', 'omnipaxos'), ('raft-rs', 'raft-rs'), ('Cassandra', 'cassandra'), ('TiKV', 'tikv'),
                   ('ZooKeeper', 'zookeeper'), ('Pineapple', 'pineapple'), ('ScalarDB', 'scalardb'), ('CockroachDB', 'crdb'),
                   ('ChainPaxos', 'chainpaxos'), ('BookKeeper', 'bk'), ('3FS', 'craq'), ('SwiftPaxos', 'swiftpaxos')):
        if pat and d.startswith(pat): return s
    return '?'
for fk in sorted(JF):
    pre, d, words, tail = fk; fam = '.'.join(x for x in fk if x)
    st = study_of(d) if not (d == 'raft-rs' and pre[:2] in ('XJ', 'XT', 'XC')) else 'MicroRaft'
    lo = leftout(d)
    if lo: LEFT[fam] = lo; continue
    if any(d == a and b in words.split('_') for a, b in EXPLOR): LEFT[fam] = 'exploratory variant (exploratory-variants.tsv)'; continue
    meas = {}
    for n, ids in JF[fk].items():
        for i in ids:
            r = recok(i)
            if r: meas[n] = (r, i)
    if not meas: LEFT[fam] = 'no measured size (only failed/queued/live joins)'; continue
    allconf = all(v[0]['verdict'] == 'conforms' for v in meas.values())
    maxi = [P for P in meas if not any(Q != P and le(P, Q) for Q in meas)]
    ax = axes_of(d) or tuple(1 for _ in next(iter(meas)))
    for P in maxi:
        r, jid = meas[P]
        cmd = ROWS[jid][0][3] if jid in ROWS else None
        if not cmd: notes[fam].append(f'{jid}: no rows command (cannot template)'); continue
        exs = [e for e in EXIDRE.findall(cmd) if EXRE.match(e) and e.count(".") >= 2]
        if len(exs) != 2: notes[fam].append(f"{jid}: join reads {len(exs)} export ids, not 2"); continue
        for k in range(len(P)):
            if not ax[k]: continue
            for s_ in (1, 2):
                C = tuple(x + (s_ if j == k else 0) for j, x in enumerate(P))
                if not valid(d, C): notes[fam].append(f'{",".join(map(str,C))} outside the domain'); continue
                jn = new_join_id(jid, C)
                if B2.get(base2(jn)): notes[fam].append(f'{",".join(map(str,C))} exists ({sorted(B2[base2(jn)])[0]})'); continue
                # exports
                blk = None; eplan = []; gmax = 1.0
                for e in exs:
                    ek = ekey(e)
                    fl = [x for x in EXF[ek] if le(x[0], C)]
                    lv = [x for x in EXL[ek] if le(x[0], C)]
                    if fl: blk = f'export {fl[0][1]} hit a limit ({fl[0][2]}) at a size <= {",".join(map(str,C))}'; break
                    if lv: blk = f'export {lv[0][1]} at a size <= {",".join(map(str,C))} is {lv[0][2]}'; break
                    tid, tcmd = export_template(e)
                    if not tcmd: blk = f'no rows template for {e}'; break
                    ncmd = sub_cmd(tcmd, C)
                    if not ncmd: blk = f'cannot substitute size into {tid}'; break
                    done = ex_best(ek, C)
                    if done:
                        t, m, basis = done[0], done[1], f'measured at C: {done[2]} {done[0]:.0f}s {done[1]:.0f}MB on {done[3]}'; g = 1.0; mr = 1.0
                        redo = 're-export (done on another node)'
                    else:
                        sp = ex_best(ek, P)
                        if not sp: blk = f'export {e} has no ok record at {",".join(map(str,P))}'; break
                        sr = step_ratio(ek, P, k)
                        if not sr:   # fallback (scaleup-1110): the largest step ratio along the same axis of another export of the same directory family
                            alt = [step_ratio(k2, P, k) for k2 in list(EX) if k2[1].split('-')[0] == ek[1].split('-')[0] and k2[0] == ek[0] and k2 != ek]
                            alt = [a for a in alt if a]
                            if alt: sr = max(alt); sr = (sr[0], sr[1], sr[2], sr[3])
                        if not sr: blk = f'no measured step of {e} along axis {k+1}'; break
                        g, mr, stp, acc = sr; g = g ** s_; mr = mr ** s_   # two steps: the ratio applied twice
                        t = max(sp[0], 1.0) * g
                        jmc = e.startswith(('XCE', 'XJE'))
                        m = sp[1] * (g ** 0.6 if (jmc and sp[1] < 500) else min(g, max(mr, 1.0)))
                        basis = f'{sp[2]} {sp[0]:.0f}s {sp[1]:.0f}MB x g={g:.2f} (step {stp[0]} {stp[2]}s -> {stp[1]} {stp[3]}s' + (f', accel x{acc:.2f}' if acc > 1 else '') + ')'
                        redo = 'new size'
                    gmax = max(gmax, g)
                    eplan.append(dict(old=e, ek=ek, cmd=ncmd, t=t, m=m, basis=basis, kind=redo, jmc=e.startswith(('XCE', 'XJE')), tid=tid, C=C))
                if blk: notes[fam].append(f'{",".join(map(str,C))} blocked: {blk}'); continue
                jt = (f(r['wall_ms']) or 1000) / 1000 * node_factor(r['node']) * gmax; jm = (f(r['peak_rss_kb']) or 0) / 1024 * gmax
                cands.append(dict(fam=fam, study=st, fk=fk, jid=jid, P=P, C=C, jn=jn, jcmd=cmd, exs=exs, eplan=eplan, jt=jt, jm=jm,
                                  jbasis=f'{jid} {(f(r["wall_ms"]) or 0)/1000:.0f}s on {r["node"]} x g={gmax:.2f}', allconf=allconf,
                                  step=s_, verdicts=sorted((n, v[0]['verdict'], v[1]) for n, v in meas.items()), axis=k))
pickle.dump(dict(cands=cands, left=LEFT, notes=dict(notes), meas={fk: {n: [(i, (recok(i) or {}).get('verdict'), (recok(i) or {}).get('wall_ms'), (recok(i) or {}).get('peak_rss_kb'), (recok(i) or {}).get('node')) for i in ids if recok(i)] for n, ids in JF[fk].items()} for fk in JF}), open(SCR + 'cands.pkl', 'wb'))
print(len(cands), 'join candidates;', len(LEFT), 'families left out;', sum(len(v) for v in notes.values()), 'notes')
