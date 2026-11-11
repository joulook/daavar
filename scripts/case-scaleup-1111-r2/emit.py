#!/usr/bin/env python3
"""case-scaleup-1111 r2 step 3 (adapted from ../case-scaleup-1111/emit.py and md.py): feasibility, groups, host class, priority, rows
files per part and class (cluster/campaign/case-scaleup-1111/r2/part<A|B|C>-<class>-pool.rows + .rows.tsv), r2/candidates.tsv and
r2/PLAN.md. Reads $SCR/cands.pkl (gen.py), $SCR/load.pkl, $SCR/stale.tsv (stale.py). Estimates are planning numbers (R10)."""
import collections, pickle, re, glob, os
G = '/local/home/mkhoshechin/Documents/github-jmc/'; SCR = '/local/home/mkhoshechin/tmp/case-scaleup-1111-r2/'
OUTR = G + 'daavar/cluster/campaign/case-scaleup-1111/r2/'; OUTP = G + 'plan/eval2/case-scaleup-1111/r2/'
X = pickle.load(open(SCR + 'cands.pkl', 'rb')); D = pickle.load(open(SCR + 'load.pkl', 'rb'))
ROWS, SIDE = D['rows'], D['side']; ALL = set(ROWS) | {r['run_id'] for r in D['recs']} | set(D['live'])
CLS = dict(brain=dict(mem=250, cap=230, heap='12g', hg=12, f=1.0), volta=dict(mem=200, cap=180, heap='16g', hg=16, f=2.0), pinky=dict(mem=80, cap=70, heap='8g', hg=8, f=2.0))
USED = set()
def fresh(i):
    b, r = i, 2
    while i in ALL or i in USED: i = f'{b}.r{r}'; r += 1
    USED.add(i); return i
PHB = ('OmniPaxos', 'raft-rs', 'MicroRaft')
kept, dropped = [], []
for c in X['cands']:
    worst = max([e['t'] for e in c['eplan']] + [c['jt']]); peak = max([e['m'] for e in c['eplan']] + [c['jm']]) / 1024
    c['worst_h'], c['peak'] = worst / 3600, peak
    if worst / 3600 > 36 or peak > 230: dropped.append(c); continue
    kept.append(c)
par = {}
def find(a):
    while par.setdefault(a, a) != a: a = par[a]
    return a
for c in kept:
    ks = [('E', e['ek'], c['C']) for e in c['eplan']]
    for k in ks[1:]: par[find(k)] = find(ks[0])
grp = collections.defaultdict(list)
for c in kept: grp[find(('E', c['eplan'][0]['ek'], c['C']))].append(c)
def jprio(c):
    if c['study'] in PHB or c['allconf']: return 1
    if any(e['jmc'] for e in c['eplan']) or c['fk'][0] != 'C3J': return 3
    return 2
def fits(cls, cs):
    K = CLS[cls]
    for c in cs:
        for e in c['eplan']:
            if e['m'] / 1024 > K['cap'] or (e['jmc'] and e['m'] / 1024 > 17 * K['hg']): return False
        if c['jm'] / 1024 > K['cap']: return False
    return True
out = collections.defaultdict(list); cand = []; gid = 0; ginfo = []
for g, cs in sorted(grp.items(), key=lambda x: -max(c['worst_h'] for c in x[1])):
    worst = max(c['worst_h'] for c in cs); peak = max(c['peak'] for c in cs)
    cls = next((k for k in ('pinky', 'volta', 'brain') if fits(k, cs) and worst * CLS[k]['f'] <= 24), None) or \
          next((k for k in ('volta', 'brain') if fits(k, cs) and worst * CLS[k]['f'] <= 36), None)
    if not cls: dropped.extend(cs); continue
    gid += 1; F = CLS[cls]['f']; prio = min(jprio(c) for c in cs)
    part = min(c['part'] for c in cs)
    exrows = {}
    for c in cs:
        for e in c['eplan']:
            k = (e['ek'], c['C'])
            if k not in exrows:
                pre, d = e['old'].split('.')[:2]; toks = e['old'].split('.')[2].split('_'); it = iter(c['C'])
                desc = '_'.join(str(next(it)) if t.isdigit() else t for t in toks)
                tt = [t for t in e['old'].split('.')[3:] if re.match(r'^[tw]\d+$', t)]
                nid = fresh(f'{pre}.{d}.{desc}.' + ('.'.join(tt) + '.' if tt else '') + 'cinf')
                cmd = e['cmd']
                if e['jmc']: cmd = f"env JAVA_TOOL_OPTIONS=-Xmx{CLS[cls]['heap']} " + cmd
                exrows[k] = (nid, e, cmd)
            e['new'] = exrows[k][0]
    gl = [f"# group r2-{part}{gid} [{cls}, part {part}, priority {prio}; est worst row {worst * F:.2f} h on {cls}, peak {peak:.1f} GB]: " +
          '; '.join(sorted({c['fam'] + ' ' + ','.join(map(str, c['P'])) + '->' + ','.join(map(str, c['C'])) for c in cs}))]
    for k, (nid, e, cmd) in exrows.items():
        gl.append(f"{nid} {CLS[cls]['mem']} 8640000 {cmd}")
        pr = 3 if e['t'] / 3600 * F > 24 else prio
        reason = f"{e['kind']}; basis {e['basis']}" + ('; 24-36 h' if e['t'] / 3600 * F > 24 else '') + ('; JMC export, 17 JVMs at -Xmx' + CLS[cls]['heap'] + ' (one at a time on pinky/volta, three on brain)' if e['jmc'] else '')
        cand.append((nid, part, cls, e['t'] / 3600 * F, e['m'] / 1024, pr, e['ek'][1], reason, e['tid'], ','.join(map(str, e['C'])), 'export'))
    for c in cs:
        jn = fresh(c['jn'])
        cmd = re.sub(r'^needok:\S+ ', '', c['jcmd']).replace('"$EVAL_OUT"/imports/', '"$EVAL_OUT"/')
        cmd = re.sub(r'(--timeout|timeout) 172800', r'\1 8640000', cmd)
        for e in c['eplan']: cmd = cmd.replace(f'"$EVAL_OUT"/{e["old"]}/export.jsonl', f'"$EVAL_OUT"/{e["new"]}/export.jsonl')
        cmd = f"needok:{','.join(e['new'] for e in c['eplan'])} " + cmd
        gl.append(f"{jn} {CLS[cls]['mem']} 8640000 {cmd}")
        pr = jprio(c); pr = max(pr, 3) if c['jt'] / 3600 * F > 24 else pr
        vs = ' '.join(f"{','.join(map(str, n))}={v}" for n, v, i in c['verdicts'])
        reason = (f"{c['found']}; " if c['found'] else '') + f"{c['fam']} {','.join(map(str, c['P']))} -> {','.join(map(str, c['C']))} (axis {c['axis'] + 1}, step +{c['step']}); measured: {vs}; join basis {c['jbasis']}" + ('; all measured sizes conform' if c['allconf'] else '') + ('; Phase B' if c['study'] in PHB else '')
        cand.append((jn, c['part'], cls, c['jt'] / 3600 * F, c['jm'] / 1024, pr, c['fam'], reason, c['jid'], ','.join(map(str, c['C'])), 'join'))
        c['newid'] = jn; c['cls'] = cls; c['gid'] = f'r2-{part}{gid}'
    out[(part, cls)].append(gl); ginfo.append((gid, cls, prio, cs))
H = "id\ttable\tfamily\tsize\tmodel\tvariant\tengine\tthreads\texpected_verdict\tnote\ttier\tkind\tspec_tool\timpl_tool\tspec_lang\timpl_lang\tmode\texploration\tbenchmark\tspec_case\timpl_case"
HC = H.split('\t'); byid = {x[0]: x for x in cand}
for f_ in glob.glob(OUTR + '*.rows*'): os.remove(f_)   # only files this script wrote (r2 directory)
for (part, cls), gls in sorted(out.items()):
    n = sum(len(g) - 1 for g in gls); tot = sum(byid[l.split(' ')[0]][3] for g in gls for l in g[1:])
    hdr = [f"# case-scaleup-1111 round 2, part {part}, {cls} pool (task 2026-11-11 about 01:55; plan/eval2/case-scaleup-1111/r2/PLAN.md, BRIEF.md): Offline only (exports and merge joins), tier cinf (no time limit), mem {CLS[cls]['mem']}; generated by scripts/case-scaleup-1111-r2/{{load,gen,stale,emit}}.py. {len(gls)} groups, {n} rows, estimated {tot:.1f} h of work (extrapolated, planning only, R10); groups in descending order of their largest row; within a group exports before their joins; a group goes to one host (its joins read the exports from that node's $EVAL_OUT).",
           f"# heaps as in scaleup-1110 (volta -Xmx16g, brain -Xmx12g, pinky -Xmx8g); at most one JMC export at a time on a pinky or a volta, three on a brain; the heap changes memory only, not the explored graphs"]
    open(OUTR + f'part{part}-{cls}-pool.rows', 'w').write('\n'.join(hdr + [l for g in gls for l in g]) + '\n')
    tl = [H]
    for g in gls:
        for l in g[1:]:
            i = l.split(' ')[0]; x = byid[i]; tpl = SIDE.get(x[8]) or {}
            row = {h: tpl.get(h, '') for h in HC}
            row.update(spec_case='', impl_case='', id=i, size=x[9], tier='cinf', note=f'case-scaleup-1111 r2 part {part} ' + ('re-export' if x[10] == 'export' and x[7].startswith('re-export') else ('stale re-proposed' if part == 'A' and x[10] == 'join' else 'next size')))
            if not tpl: row.update(family=x[6], kind='export' if x[10] == 'export' else 'join', mode='offline-batch', exploration='full')
            tl.append('\t'.join(row[h] for h in HC))
    open(OUTR + f'part{part}-{cls}-pool.rows.tsv', 'w').write('\n'.join(tl) + '\n')
cand.sort(key=lambda x: (x[5], x[3], x[0]))
open(OUTP + 'candidates.tsv', 'w').write('id\tpart\thost_class\test_h\test_mem_gb\tpriority\tfamily\treason\n' + ''.join(
    f"{x[0]}\t{x[1]}\t{x[2]}\t{x[3]:.2f}\t{x[4]:.2f}\t{x[5]}\t{x[6]}\t{x[7]}\n" for x in cand))
pickle.dump(dict(cand=cand, ginfo=ginfo, dropped=dropped, kept=kept), open(SCR + 'emit.pkl', 'wb'))
print(len(cand), 'rows;', sorted(collections.Counter((x[1], x[2], x[5]) for x in cand).items()), '; dropped joins', len(dropped))
