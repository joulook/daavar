#!/usr/bin/env python3
"""case-scaleup-1111 round 3 (coordinator 2026-11-11): A Neon next sizes, B TiKV scenarios without an ok record (not running, not
pending), C the 24 r2 'later' joins that waited on the r2 cassandra-cas/derived exports (now on pinky19/pinky20). Writes
cluster/campaign/case-scaleup-1111/r3/<host>.rows(.rows.tsv) and SUMMARY.md. Helpers: the r2 gen.py helper half (exec, scratch r3).
Estimates are planning numbers (R10); time on pinky/volta = brain-equivalent x 2."""
import collections, glob, os, pickle, re, sys
S2 = '/local/home/mkhoshechin/Documents/github-jmc/daavar/scripts/case-scaleup-1111-r2/gen.py'
src = open(S2).read().replace("tmp/case-scaleup-1111-r2/'", "tmp/case-scaleup-1111-r3/'")
exec(src.split('# ======== round 2 ========')[0]); exec(src.split('# ======== round 2 ========')[1].split('JF = collections')[0])
SW = '/local/home/mkhoshechin/tmp/sweep-1111/'; CA = G + 'daavar/cluster/campaign/case-scaleup-1111/'; OUT = CA + 'r3/'
IDLE = ['pinky04', 'pinky12', 'pinky16', 'pinky18', 'pinky20', 'pinky21', 'pinky22', 'pinky23', 'pinky24', 'pinky25', 'pinky27', 'pinky28',
        'pinky29', 'pinky30', 'pinky31', 'volta07', 'volta10', 'volta11', 'volta14']
CLS = dict(brain=dict(mem=250, cap=230, heap='12g', hg=12, f=1.0), volta=dict(mem=200, cap=180, heap='16g', hg=16, f=2.0), pinky=dict(mem=80, cap=70, heap='8g', hg=8, f=2.0))
hclass = lambda h: 'brain' if h.startswith('brain') else ('volta' if h.startswith('volta') else 'pinky')
# ---- host map: every id placed, running or pending on a host
HOST = collections.defaultdict(set)
def addfile(fn, h=None, groups=None):
    gi = -1
    for l in open(fn):
        if l.startswith('# group'): gi += 1; continue
        if not l.strip() or l.startswith('#'): continue
        hh = h if h else (groups[gi] if groups and 0 <= gi < len(groups) else None)
        if hh: HOST[l.split(' ', 1)[0]].add(hh)
for fn in glob.glob(CA + 'nodes/*.rows'): addfile(fn, os.path.basename(fn)[:-5])
for fn in glob.glob(CA + 'r2/later/*.rows'): addfile(fn, os.path.basename(fn)[:-5])
addfile(CA + 'r2/partA-pinky-pool.rows', groups=['pinky19', 'pinky20']); addfile(CA + 'r2/partB-pinky-pool.rows', groups=['pinky21', 'pinky23'])
addfile(CA + 'r2/partA-brain-pool.rows', 'brain15')
for l in open(SW + 'running-live.tsv'):
    t = l.rstrip('\n').split('\t')
    if len(t) > 3 and t[2] == 'CASE': HOST[t[3]].add(t[1])
for l in open(SW + 'pending-dedup.tsv'):
    t = l.rstrip('\n').split('\t')
    if len(t) > 4 and t[2] == 'CASE': HOST[t[4]].add(t[1])
QUEUED = set(HOST)
ALL2 = ALL | QUEUED
OKON = collections.defaultdict(set)
for i in ALL:
    for r in R.get(i, []):
        if r['stop_reason'] == 'ok': OKON[i].add(r['node'])
USED = set()
def fresh(b):
    i, n = b, 2
    while i in ALL2 or i in USED: i = f'{b}.r{n}'; n += 1
    USED.add(i); return i
def fresh_re(b):
    n = 2
    while f'{b}.r{n}' in ALL2 or f'{b}.r{n}' in USED: n += 1
    USED.add(f'{b}.r{n}'); return f'{b}.r{n}'
def okrec(i):
    v = [r for r in R.get(i, []) if r['stop_reason'] == 'ok']; return v[-1] if v else None
def bt(r): return (f(r['wall_ms']) or 0) / 1000 * node_factor(r['node'])
def bm(r): return (f(r['peak_rss_kb']) or 0) / 1024
GROUPS = []   # dict(part, name, rows=[(id, cmd, sidecar_tid, size, note, kind, est_brain_s, mem_mb, jmc)], host=None|fixed)
LEFT = []
# ======== C: the 24 r2 later joins waiting on the r2 cassandra exports ========
X2 = pickle.load(open('/local/home/mkhoshechin/tmp/case-scaleup-1111-r2/cands.pkl', 'rb'))
def est(e, P, C, k, s_):
    ek = ekey(e); done = ex_best(ek, C)
    if done: return done[0], done[1], f'measured at C: {done[2]} {done[0]:.0f}s {done[1]:.0f}MB on {done[3]}'
    sp = ex_best(ek, P)
    if not sp: return None
    sr = step_ratio(ek, P, k)
    if not sr:
        alt = [a for a in (step_ratio(k2, P, k) for k2 in list(EX) if k2[1].split('-')[0] == ek[1].split('-')[0] and k2[0] == ek[0] and k2 != ek) if a]
        if alt: sr = max(alt)
    if not sr: return None
    g, mr, stp, acc = sr; g = g ** s_; mr = mr ** s_
    return max(sp[0], 1.0) * g, sp[1] * min(g, max(mr, 1.0)), f'{sp[2]} {sp[0]:.0f}s {sp[1]:.0f}MB x g={g:.2f} (step {stp[0]} -> {stp[1]})'
NEWEX = {}
byhost = collections.defaultdict(list)
for l in X2['later']:
    m = re.match(r'export of (\S+) at (\S+) proposed in this round', l['why'])
    if not m: continue
    tag = f"C {l['fam']} {','.join(map(str, l['P']))} -> {','.join(map(str, l['C']))}"
    jid = l['jid']; cmd = ROWS[jid][0][3]; r = okrec(jid)
    exs = [e for e in EXIDRE.findall(cmd) if EXRE.match(e) and e.count('.') >= 2]
    wsz = tuple(int(x) for x in m.group(2).split(','))
    W = [i for i in QUEUED if EXRE.match(i) and i.split('.')[1] == m.group(1) and nums(i) == wsz and any(ekey(i) == ekey(e) for e in exs) and HOST.get(i)]
    if not W: LEFT.append((tag, f'infeasible: the awaited r2 export of {m.group(1)} at {m.group(2)} was dropped by r2 emit as over budget (r2 PLAN.md "Over the budget"), so it is queued nowhere')); continue
    W = sorted(W)[0]; h = sorted(HOST[W])[0]; K = CLS[hclass(h)]
    P, C = l['P'], l['C']; k = next(j for j in range(len(P)) if P[j] != C[j]); s_ = C[k] - P[k]
    plan, bad, gmax, rows = [], None, 1.0, []
    for e in exs:
        ek = ekey(e)
        use = sorted(i for i in QUEUED | set(OKON) if EXRE.match(i) and ekey(i) == ek and nums(i) == C and (h in HOST.get(i, ()) or h in OKON.get(i, ())))
        if (h, ek, C) in NEWEX: use = [NEWEX[(h, ek, C)]]
        if use: plan.append((e, use[0])); continue
        tid, tcmd = export_template(e); ncmd = sub_cmd(tcmd, C) if tcmd else None; es = est(e, P, C, k, s_)
        if not ncmd or not es: bad = f'cannot build or estimate the re-export of {e} at {C}'; break
        t, mm, basis = es
        if t / 3600 * K['f'] > 36 or mm / 1024 > K['cap']: bad = f're-export of {e} at {C} estimated {t / 3600 * K["f"]:.1f} h, {mm / 1024:.1f} GB on {h}: over the limit'; break
        gmax = max(gmax, t / max(1.0, ex_best(ek, P)[0])) if not ex_best(ek, C) else gmax
        nid = fresh_re(new_export_id(e, C)); NEWEX[(h, ek, C)] = nid
        jmc_ = e.startswith(('XCE', 'XJE')); ncmd = re.sub(r'^env JAVA_TOOL_OPTIONS=\\S+ ', '', ncmd)
        rows.append((nid, ncmd, tid, ','.join(map(str, C)), f'case-scaleup-1111 r3 part C re-export on {h}; basis {basis}', 'export', t, mm, jmc_)); plan.append((e, nid))
    if bad: LEFT.append((tag, bad)); continue
    jt = bt(r) * gmax; jm = bm(r) * gmax
    jn = fresh(new_join_id(jid, C))
    jc = re.sub(r'^needok:\S+ ', '', cmd).replace('"$EVAL_OUT"/imports/', '"$EVAL_OUT"/')
    for e, u in plan: jc = jc.replace(f'"$EVAL_OUT"/{e}/export.jsonl', f'"$EVAL_OUT"/{u}/export.jsonl')
    need = [W] + [u for e, u in plan if u != W]
    rows.append((jn, f"needok:{','.join(need)} " + jc, jid, ','.join(map(str, C)), f'case-scaleup-1111 r3 part C: waits for {W} on {h}' + ('; re-export' if len(rows) else ''), 'join', jt, jm, False))
    byhost[h] += rows
for h, rs in byhost.items(): GROUPS.append(dict(part='C', name=f'C-{h}', rows=rs, host=h))
for g in GROUPS: pass
# ======== A: Neon ========
NJ = 'C3J.neon-sk.impl_correct_3_1_1_fifo.cinf.r3.m'; NI = 'C3E.neon-sk.impl_correct_3_1_1_fifo.t16.cinf'; NS = 'C3E.neon-sk-tla.spec_3_1_1_fifo.t16.cinf'
def neon_cmds(v, c):
    ic = sub_cmd(ROWS[NI][0][3], c).replace(' -- correct ', f' -- {v} '); sc = sub_cmd(ROWS[NS][0][3], c)
    return ic, sc
m311 = okrec('C3E.neon-sk.impl_correct_3_1_1_fifo.t16.cinf.r3'); m211 = okrec('C3E.neon-sk.impl_correct_2_1_1_fifo.t16.cinf.r3'); m111 = okrec('C3E.neon-sk.impl_correct_1_1_1_fifo.t16.cinf.r3')
NEON = []   # (variant, size, est_s, mem_mb, basis)
for c in ((1, 2, 1), (1, 3, 1), (1, 1, 2), (1, 2, 2), (1, 1, 3)):
    NEON.append(('correct', c, 3600.0, 1024.0, f'unmeasured axis; n=1 measured C3E.neon-sk.impl_correct_1_1_1_fifo.t16.cinf.r3 {bt(m111):.2f}s; assumed <= 1 h, 1 GB'))
NEON.append(('correct', (2, 1, 2), 3 * 3600.0, 2048.0, f'unmeasured axis r; C3E.neon-sk.impl_correct_2_1_1_fifo.t16.cinf.r3 {bt(m211):.2f}s; assumed <= 3 h, 2 GB'))
for v in ('bad-quorum', 'no-prev-term-rule', 'wrong-donor'):
    NEON.append((v, (2, 1, 1), max(bt(m211), 1.0), bm(m211), f'as correct: C3E.neon-sk.impl_correct_2_1_1_fifo.t16.cinf.r3 {bt(m211):.2f}s {bm(m211):.0f}MB on brain13'))
    NEON.append((v, (3, 1, 1), bt(m311), bm(m311), f'as correct: C3E.neon-sk.impl_correct_3_1_1_fifo.t16.cinf.r3 {bt(m311):.0f}s {bm(m311):.0f}MB on brain13'))
LEFT.append(('A neon-sk correct 4,1,1', f'infeasible: step C3E.neon-sk.impl_correct_2_1_1_fifo.t16.cinf.r3 {bt(m211):.2f}s -> C3E.neon-sk.impl_correct_3_1_1_fifo.t16.cinf.r3 {bt(m311):.0f}s (x{bt(m311) / max(bt(m211), 0.01):.0f}); 4,1,1 >= 2000 h'))
LEFT.append(('A neon-sk correct/bad-quorum 3,2,1 and above on p', 'blocked: 2,2,1 exports running on brain13 (C3E.neon-sk.impl_{correct,bad-quorum}_2_2_1_fifo.t16.cinf.r3); 3,2,1 >= 1e15 receive orders (REVIEW-astra-rust.md finding 8, STUDY-NOTES 2026-11-10 18:38)'))
LEFT.append(('A neon-sk no-prev-term-rule / wrong-donor 2,2,1', 'later: no estimate until the correct 2,2,1 export on brain13 lands (running about 6 h)'))
LEFT.append(('A neon-sk correct 3,1,2 and 2,1,3', 'later: waits for the r-axis step of 1,1,2 / 2,1,2 proposed here'))
LEFT.append(('A neon-sk faults at n=1', 'left out: a quorum fault needs n >= 2 safekeepers (assumption, not checked against the harness)'))
ng = collections.defaultdict(list)
for v, c, t, mm, basis in NEON:
    ic, sc = neon_cmds(v, c); s = '_'.join(map(str, c)); zs = ','.join(map(str, c))
    jid_old = f'C3J.neon-sk.impl_{v}_{s}_fifo'
    if any(base2(i) == base2(jid_old + '.cinf.m') and (okrec(i) or i in QUEUED) for i in ALL2): LEFT.append((f'A neon-sk {v} {zs}', 'exists (ok, running or pending)')); continue
    key = 'small' if c[0] <= 2 and v == 'correct' else (f'{v}' if c[0] == 3 else 'small2')
    gname = {'small': 'A-neon-correct-probes', 'small2': 'A-neon-faults-2', }.get(key, f'A-neon-{v}-3')
    ng[gname].append((v, c, ic, sc, t, mm, basis))
for gname, items in ng.items():
    rows = []; spec_ids = {}
    for v, c, ic, sc, t, mm, basis in items:
        s = '_'.join(map(str, c)); zs = ','.join(map(str, c))
        if c not in spec_ids:
            sid = fresh(f'C3E.neon-sk-tla.spec_{s}_fifo.t16.cinf'); spec_ids[c] = sid
            rows.append((sid, sc, NS, zs, 'case-scaleup-1111 r3 part A (Neon) spec export; spec exports measured <= 0.03 s (C3E.neon-sk-tla.spec_3_2_1_fifo.t16.cinf)', 'export', 1.0, 5.0, False))
        iid = fresh(f'C3E.neon-sk.impl_{v}_{s}_fifo.t16.cinf')
        rows.append((iid, ic, NI, zs, f'case-scaleup-1111 r3 part A (Neon) impl export; basis {basis}', 'export', t, mm, False))
        jn = fresh(f'C3J.neon-sk.impl_{v}_{s}_fifo.cinf.m')
        jc = re.sub(r'^needok:\S+ ', '', ROWS[NJ][0][3]); te = EXIDRE.findall(jc)
        for e_ in te: jc = jc.replace(f'"$EVAL_OUT"/{e_}/export.jsonl', f'"$EVAL_OUT"/{spec_ids[c] if ".spec_" in e_ else iid}/export.jsonl')
        rows.append((jn, f'needok:{spec_ids[c]},{iid} ' + jc, NJ, zs, f'case-scaleup-1111 r3 part A (Neon) join {v} {zs}; joins measured <= 0.01 s (C3J.neon-sk.impl_correct_3_1_1_fifo.cinf.r3.m)', 'join', 1.0, 2.0, False))
    GROUPS.append(dict(part='A', name=gname, rows=rows, host=None))
# ======== B: TiKV scenarios ========
TK = re.compile(r'^(C3J|XJ1|XC1|XC2)\.tikv-txn')
def tkind(i):
    p = i.split('.'); return (p[0], p[1], '.'.join(t for t in p[3:] if not SUF.match(t)))
def tscen(i): return tuple(i.split('.')[2].split('_')[1:])
joins = [i for i in ALL2 if TK.match(i) and i.endswith('.m')]
U = sorted({tscen(i) for i in joins})
kinds = collections.defaultdict(list)
for i in joins: kinds[tkind(i)].append(i)
tex = [i for i in ALL2 if re.match(r'^(C3E|XCE)\.tikv-txn', i)]
def ex_for(e_old, sc):   # the export id of the same export kind at scenario sc (spec exports drop the fault token)
    p = e_old.split('.'); spec = p[2].startswith('spec')
    sc2 = tuple(t for t in sc if not (spec and re.match(r'^f\d+$', t)))
    cand = [i for i in tex if i.split('.')[:2] == p[:2] and i.split('.')[2].split('_')[0] == p[2].split('_')[0] and tscen(i) == sc2]
    okc = [i for i in cand if okrec(i)]
    return (sorted(okc) or sorted(cand) or [None])[0], sc2
def ex_cost(i, sc):
    r = okrec(i) if i else None
    if r: return bt(r), bm(r), f'measured {i} {bt(r):.0f}s {bm(r):.0f}MB on {r["node"]}'
    return None
tgroups = collections.defaultdict(list)
for kd, ids in sorted(kinds.items()):
    tmpl = sorted([i for i in ids if okrec(i) and i in ROWS] or [i for i in ids if i in ROWS])
    if not tmpl: continue
    tj = tmpl[0]; tcmd = ROWS[tj][0][3]; texs = [e for e in EXIDRE.findall(tcmd) if EXRE.match(e)]
    for sc in U:
        have = [i for i in ids if tscen(i) == sc]
        tag = f"B {kd[0]}.{kd[1]}{'.' + kd[2] if kd[2] else ''} {'_'.join(sc)}"
        if any(okrec(i) for i in have) or any(i in QUEUED for i in have): continue
        fails = [(i, x['stop_reason']) for i in have for x in R.get(i, [])]
        rep = [(i, x) for i in have for x in R.get(i, []) if fail_repeats(x)]
        if rep: LEFT.append((tag, f'record {rep[0][0]}: {fail_repeats(rep[0][1])} (repeats)')); continue
        plan = []; bad = None
        for e in texs:
            ne, sc2 = ex_for(e, sc)
            if not ne or ne not in ROWS:
                # no export id at this scenario anywhere: build from the template by token substitution
                tid = e; cmd0 = ROWS[e][0][3] if e in ROWS else None
                if not cmd0: bad = f'no rows command for {e}'; break
                o = tscen(e); oc = ','.join(o); nc = ','.join(sc2)
                cmd0 = cmd0.replace(','.join(o), nc).replace(' -- ' + ' '.join(o), ' -- ' + ' '.join(sc2))
                if 'jmc-run' in cmd0: cmd0 = cmd0.replace(':impl,' + oc, ':impl,' + nc)
                p = e.split('.'); base = f"{p[0]}.{p[1]}.{p[2].split('_')[0]}_{'_'.join(sc2)}." + '.'.join(t for t in p[3:] if re.match(r'^[tw]\d+$', t)) + '.cinf'
                cost = None
            else:
                tid = ne; cmd0 = ROWS[ne][0][3]; p = ne.split('.')
                base = f"{p[0]}.{p[1]}.{p[2]}." + '.'.join(t for t in p[3:] if re.match(r'^[tw]\d+$', t)) + '.cinf'
                cost = ex_cost(ne, sc)
            if not cost:   # closest run scenario: same export kind, same scenario name, else the largest of the kind
                pk = e.split('.')[:2] + [e.split('.')[2].split('_')[0]]
                sib = [(bt(okrec(i)), bm(okrec(i)), i) for i in tex if i.split('.')[:2] == pk[:2] and i.split('.')[2].split('_')[0] == pk[2] and okrec(i)]
                same = [s for s in sib if tscen(s[2])[0] == sc2[0]]
                pick = max(same or sib) if (same or sib) else None
                if not pick: bad = f'no measured scenario of {e.split(".")[1]}'; break
                cost = (pick[0], pick[1], f'closest run scenario {pick[2]} {pick[0]:.0f}s {pick[1]:.0f}MB ({"same scenario name" if same else "largest of the kind"})')
            cmd0 = re.sub(r'^env JAVA_TOOL_OPTIONS=\S+ ', '', cmd0)
            cmd0 = re.sub(r'(export\.jsonl) 172800', r'\1 8640000', cmd0)
            plan.append(dict(old=e, base=base, cmd=cmd0, tid=tid, t=cost[0], m=cost[1], basis=cost[2], jmc=e.startswith('XCE'), sc2=sc2))
        if bad: LEFT.append((tag, bad)); continue
        jr = okrec(tj); jt = max(bt(jr), 1.0); jm = bm(jr)
        jbase = f"{kd[0]}.{kd[1]}.impl_{'_'.join(sc)}." + (kd[2] + '.' if kd[2] else '') + 'cinf.m'
        tgroups['_'.join(sc)].append(dict(tag=tag, kd=kd, tj=tj, tcmd=tcmd, plan=plan, jbase=jbase, jt=jt, jm=jm, fails=fails, sc=sc))
for scn, js in sorted(tgroups.items()):
    rows = []; made = {}
    for j in js:
        for p in j['plan']:
            if p['base'] not in made:
                nid = fresh_re(p['base']) if (p['base'] in ALL2) else fresh(p['base']); made[p['base']] = nid
                rows.append((nid, p['cmd'], p['tid'], ','.join(p['sc2']), f"case-scaleup-1111 r3 part B (TiKV) re-export; basis {p['basis']}", 'export', p['t'], p['m'], p['jmc']))
            p['new'] = made[p['base']]
        jc = re.sub(r'^needok:\S+ ', '', j['tcmd']).replace('"$EVAL_OUT"/imports/', '"$EVAL_OUT"/')
        for p in j['plan']: jc = jc.replace(f'"$EVAL_OUT"/{p["old"]}/export.jsonl', f'"$EVAL_OUT"/{p["new"]}/export.jsonl')
        jn = fresh(j['jbase'])
        st = ('; earlier records: ' + '; '.join(f'{i} {s}' for i, s in j['fails'][:2])) if j['fails'] else '; no record, not running, not pending'
        rows.append((jn, f"needok:{','.join(p['new'] for p in j['plan'])} " + jc, j['tj'], '_'.join(j['sc']), f"case-scaleup-1111 r3 part B (TiKV) new scenario pair {j['tag'][2:]}; join basis {j['tj']}{st}", 'join', j['jt'], j['jm'], False))
    GROUPS.append(dict(part='B', name=f'B-tikv-{scn}', rows=rows, host=None))
# ======== placement ========
load = collections.defaultdict(float); placed = collections.defaultdict(list)
for g in GROUPS:
    g['h'] = sum(r[6] for r in g['rows']) / 3600; g['peak'] = max(r[7] for r in g['rows']) / 1024
    g['jmcpeak'] = max([r[7] for r in g['rows'] if r[8]] or [0]) / 1024
for g in sorted(GROUPS, key=lambda g: -g['h']):
    if g['host']: placed[g['host']].append(g); load[g['host']] += g['h'] * CLS[hclass(g['host'])]['f']; continue
    ok = []
    for h in IDLE:
        K = CLS[hclass(h)]
        if g['peak'] > K['cap'] or g['jmcpeak'] > K['hg'] * 0.9 or g['h'] * K['f'] > 36: continue
        ok.append(h)
    if not ok: LEFT.append((f"{g['part']} group {g['name']}", f"no idle host fits: {g['h']:.1f} brain-h, peak {g['peak']:.1f} GB, JMC peak {g['jmcpeak']:.1f} GB (pinky heap 8g, volta 16g; tikv XCE is one JVM, w1)")); continue
    empty = [h for h in ok if not placed[h]]
    h = (sorted(empty, key=lambda h: (hclass(h) != 'pinky' if g['jmcpeak'] < 7 else hclass(h) != 'volta', h)) or sorted(ok, key=lambda h: load[h]))[0]
    placed[h].append(g); load[h] += g['h'] * CLS[hclass(h)]['f']
H_ = "id\ttable\tfamily\tsize\tmodel\tvariant\tengine\tthreads\texpected_verdict\tnote\ttier\tkind\tspec_tool\timpl_tool\tspec_lang\timpl_lang\tmode\texploration\tbenchmark\tspec_case\timpl_case"
HC = H_.split('\t')
for fn in glob.glob(OUT + '*.rows*'): os.remove(fn)
summ = []
for h in sorted(h for h in placed if placed[h]):
    K = CLS[hclass(h)]; L = [f"# case-scaleup-1111 round 3 rows for {h} (coordinator 2026-11-11; plan/eval2/case-scaleup-1111/r3/, scripts/case-scaleup-1111-r3/r3.py): Offline only, tier cinf, mem {K['mem']}; groups: " + ', '.join(g['name'] for g in placed[h]) + f"; est {load[h]:.1f} h on {hclass(h)} (planning only, R10); exports before their joins; one JMC export at a time on this host (dispatcher)",
                             f"# heap -Xmx{K['heap']} for JMC exports"]
    T = [H_]; nr = 0
    for g in placed[h]:
        L.append(f"# group {g['name']} [part {g['part']}; est {g['h'] * K['f']:.2f} h on {hclass(h)}, peak {g['peak']:.2f} GB]")
        for i, cmd, tid, size, note, kind, t, mm, jmc in g['rows']:
            if jmc: cmd = f"env JAVA_TOOL_OPTIONS=-Xmx{K['heap']} " + cmd
            L.append(f"{i} {K['mem']} 8640000 {cmd}"); nr += 1
            tpl = SIDE.get(tid) or {}; row = {x: tpl.get(x, '') for x in HC}
            row.update(spec_case='', impl_case='', id=i, size=size, tier='cinf', note=note + f'; est {t / 3600 * K["f"]:.2f} h {mm / 1024:.2f} GB on {hclass(h)}')
            if not tpl: row.update(kind=kind, mode='offline-batch', exploration='full', family=i.split('.')[1])
            T.append('\t'.join(row[x] for x in HC))
    open(OUT + f'{h}.rows', 'w').write('\n'.join(L) + '\n'); open(OUT + f'{h}.rows.tsv', 'w').write('\n'.join(T) + '\n')
    summ.append((h, hclass(h), nr, sum(1 for g in placed[h] for r in g['rows'] if r[5] == 'join'), load[h], ', '.join(f"{g['part']}:{g['name']}" for g in placed[h])))
pickle.dump(dict(GROUPS=GROUPS, LEFT=LEFT, summ=summ, U=U), open(SCR + 'r3.pkl', 'wb'))
S = ['# Round 3: summary (generated by daavar/scripts/case-scaleup-1111-r3/r3.py; estimates are planning numbers, R10)', '',
     'Rows: daavar/cluster/campaign/case-scaleup-1111/r3/<host>.rows (+ .rows.tsv). Parts: A Neon next sizes, B TiKV scenario pairs without an ok record / not running / not pending, C the 24 r2 later joins (cassandra-cas/derived) on pinky19/pinky20.', '',
     '| host | class | rows | joins | est h (class) | groups |', '|---|---|---|---|---|---|']
for s in summ: S.append(f'| {s[0]} | {s[1]} | {s[2]} | {s[3]} | {s[4]:.1f} | {s[5]} |')
S += ['', f'Total {sum(s[2] for s in summ)} rows, {sum(s[3] for s in summ)} joins, est {sum(s[4] for s in summ):.1f} h. Idle hosts without rows: ' + (', '.join(h for h in IDLE if not placed.get(h)) or 'none') + '.', '',
      '## Assumptions', '',
      '1. Host map: round-1 nodes/<host>.rows, r2/later/<host>.rows, r2 Part A pinky groups 1/2 -> pinky19/pinky20, Part B groups 1/2 -> pinky21/pinky23, Part A brain -> brain15, plus running-live.tsv/pending-dedup.tsv CASE rows. All of these count as queued (not proposed again).',
      '2. Neon (A): axes `<n> <p> <r>`, domain p*r <= 255 (INTERFACE.md, REVIEW-astra-rust.md). Sizes along unmeasured axes (p at n=1, r) carry assumed bounds (stated per row), not extrapolations. Fault variants (bad-quorum, no-prev-term-rule, wrong-donor) at 2,1,1 and 3,1,1 cost as the correct export at the same size. Spec exports and joins of Neon are measured <= 0.03 s.',
      f'3. TiKV (B): the scenario universe is the {len(U)} scenario tuples of any TiKV join id (scenario, async, onepc, a, model, fault): ' + '; '.join('_'.join(u) for u in U) + '. Pair kinds: C3J vs TLA / vs-plus, XJ1 vs TLA / vs-plus, XC1, XC2. A kind x scenario is new when no id of it has an ok record and none is queued. Export estimate: its own ok record (any node), else the closest run scenario (same export kind, same scenario name; else the largest of the kind). TiKV XCE rows keep their measured shape (one JVM, w1, no --split); heap -Xmx8g on pinky / -Xmx16g on volta replaces the earlier -Xmx32g: a JMC export whose estimate exceeds 0.9 x the heap goes to a volta or is left out.',
      '4. C: each join gets needok on the r2 export it waited for; the other inputs at its size are re-exported on the same host (fresh .cinf.rN ids). Estimates as gen.py (step ratio from the measured size).',
      '5. Placement: one group per idle host while hosts remain (largest group first; non-JMC on pinky, large JMC on volta), then the least-loaded fitting host. Limits: <= 36 h on the class (x2 for pinky/volta), peak <= 70/180 GB.', '',
      '## Left out', '']
for t, w in LEFT: S.append(f'- {t}: {w}')
open(OUT + 'SUMMARY.md', 'w').write('\n'.join(S) + '\n')
print(len(GROUPS), 'groups', sum(s[2] for s in summ), 'rows on', len(summ), 'hosts', f'{sum(s[4] for s in summ):.1f} h', len(LEFT), 'left')
for s in summ: print(s)
