#!/usr/bin/env python3
"""case-scaleup-1111 step 3: feasibility, grouping, host class, priority; writes the rows files, sidecars, candidates.tsv and
PLAN.md. Reads $SCR/cands.pkl (gen.py). Estimates are planning numbers (R10)."""
import collections, pickle, re, sys
sys.path.insert(0, '/local/home/mkhoshechin/Documents/github-jmc/daavar/scripts/case-scaleup-1111')
G = '/local/home/mkhoshechin/Documents/github-jmc/'; SCR = '/local/home/mkhoshechin/tmp/case-scaleup-1111/'
OUTR = G + 'daavar/cluster/campaign/case-scaleup-1111/'; OUTP = G + 'plan/eval2/case-scaleup-1111/'
X = pickle.load(open(SCR + 'cands.pkl', 'rb')); D = pickle.load(open(SCR + 'load.pkl', 'rb'))
ROWS, SIDE = D['rows'], D['side']; ALL = set(ROWS) | {r['run_id'] for r in D['recs']} | set(D['live'])
CLS = dict(brain=dict(mem=250, cap=230, heap='12g', f=1.0), volta=dict(mem=200, cap=180, heap='16g', f=2.0), pinky=dict(mem=80, cap=70, heap='8g', f=2.0))
USED = set()
def fresh(i):
    b, r = i, 2
    while i in ALL or i in USED: i = f'{b}.r{r}'; r += 1
    USED.add(i); return i
PHB = ('OmniPaxos', 'raft-rs', 'MicroRaft')
# ---- per join: feasibility on a brain node (time measured on brain-equivalent seconds)
kept, dropped = [], []
for c in X['cands']:
    chain = sum(e['t'] for e in c['eplan']) + c['jt']
    worst = max([e['t'] for e in c['eplan']] + [c['jt']]); peak = max([e['m'] for e in c['eplan']] + [c['jm']]) / 1024
    if c['step'] == 2 and chain / 3600 > 12: continue          # second step only when cheap (scaleup-1110)
    if worst / 3600 > 36 or peak > 230: dropped.append((c, worst / 3600, peak)); continue
    c['worst_h'], c['peak'] = worst / 3600, peak; kept.append(c)
# ---- groups: joins sharing an export (key, size) go together
par = {}
def find(a):
    while par.setdefault(a, a) != a: a = par[a]
    return a
for c in kept:
    ks = [('E', e['ek'], c['C']) for e in c['eplan']]
    for k in ks[1:]: par[find(k)] = find(ks[0])
grp = collections.defaultdict(list)
for c in kept: grp[find(('E', c['eplan'][0]['ek'], c['C']))].append(c)
out = collections.defaultdict(list); cand = []; gid = 0; ginfo = []
for g, cs in sorted(grp.items(), key=lambda x: -max(c['worst_h'] for c in x[1])):
    gid += 1
    worst = max(c['worst_h'] for c in cs); peak = max(c['peak'] for c in cs)
    cls = next((k for k in ('pinky', 'volta', 'brain') if peak <= CLS[k]['cap'] and worst * CLS[k]['f'] <= 24), None)
    if not cls: cls = next((k for k in ('volta', 'brain') if peak <= CLS[k]['cap'] and worst * CLS[k]['f'] <= 36), None)
    if not cls: dropped.extend((c, c['worst_h'], c['peak']) for c in cs); continue
    F = CLS[cls]['f']
    def jprio(c):
        if c['study'] in PHB or c['allconf']: return 1
        if any(e['jmc'] for e in c['eplan']) or c['fk'][0] != 'C3J': return 3
        return 2
    prio = min(jprio(c) for c in cs)
    # ids (computed without importing gen, which re-runs the generator)
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
    gl = [f"# group {gid} [{cls}, priority {prio}; est worst row {worst * F:.2f} h on {cls}, peak {peak:.1f} GB]: " + '; '.join(sorted({c['fam'] + ' ' + ','.join(map(str, c['P'])) + '->' + ','.join(map(str, c['C'])) for c in cs}))]
    for k, (nid, e, cmd) in exrows.items():
        gl.append(f"{nid} {CLS[cls]['mem']} 8640000 {cmd}")
        pr = 3 if e['t'] / 3600 * F > 24 else prio
        reason = f"{e['kind']}; basis {e['basis']}" + ('; 24-36 h' if e['t'] / 3600 * F > 24 else '') + ('; JMC export (one at a time on pinky/volta, three on brain)' if e['jmc'] else '')
        cand.append((nid, cls, e['t'] / 3600 * F, e['m'] / 1024, pr, e['ek'][1], reason, e['tid'], ','.join(map(str, e['C'])), 'export'))
    for c in cs:
        jn = fresh(c['jn'])
        cmd = re.sub(r'^needok:\S+ ', '', c['jcmd']).replace('"$EVAL_OUT"/imports/', '"$EVAL_OUT"/')
        for e in c['eplan']: cmd = cmd.replace(f'"$EVAL_OUT"/{e["old"]}/export.jsonl', f'"$EVAL_OUT"/{e["new"]}/export.jsonl')
        cmd = f"needok:{','.join(e['new'] for e in c['eplan'])} " + cmd
        gl.append(f"{jn} {CLS[cls]['mem']} 8640000 {cmd}")
        pr = jprio(c); pr = max(pr, 3) if c['jt'] / 3600 * F > 24 else pr
        vs = ' '.join(f"{','.join(map(str, n))}={v}" for n, v, i in c['verdicts'])
        reason = f"next size of {c['fam']} ({','.join(map(str, c['P']))} -> {','.join(map(str, c['C']))}, axis {c['axis'] + 1}, step {c['step']}); measured: {vs}; join basis {c['jbasis']}" + ('; all measured sizes conform' if c['allconf'] else '') + ('; Phase B' if c['study'] in PHB else '')
        cand.append((jn, cls, c['jt'] / 3600 * F, c['jm'] / 1024, pr, c['fam'], reason, c['jid'], ','.join(map(str, c['C'])), 'join'))
        c['newid'] = jn; c['cls'] = cls
    out[cls].append(gl); ginfo.append((gid, cls, prio, cs))
# ---- write rows files and sidecars
H = "id\ttable\tfamily\tsize\tmodel\tvariant\tengine\tthreads\texpected_verdict\tnote\ttier\tkind\tspec_tool\timpl_tool\tspec_lang\timpl_lang\tmode\texploration\tbenchmark\tspec_case\timpl_case"
HC = H.split('\t'); byid = {x[0]: x for x in cand}
for cls, gls in out.items():
    n = sum(len(g) - 1 for g in gls); tot = sum(byid[l.split(' ')[0]][2] for g in gls for l in g[1:])
    hdr = [f"# case-scaleup-1111 {cls} pool (owner 2026-11-11 01:05, plan/eval2/case-scaleup-1111/BRIEF.md): next sizes of the 13 case studies, Offline only (exports and merge joins), tier cinf (no time limit), mem {CLS[cls]['mem']}; generated by scripts/case-scaleup-1111/{{load,gen,emit}}.py. {len(gls)} groups, {n} rows, estimated {tot:.1f} h of work (extrapolated, planning only, R10); groups in descending order of their largest row; within a group exports before their joins; a group goes to one host (its joins read the exports from that node's $EVAL_OUT).",
           f"# heaps as in scaleup-1110 (volta -Xmx16g, brain -Xmx12g, pinky -Xmx8g); at most one JMC export at a time on a pinky or a volta, three on a brain; the heap changes memory only, not the explored graphs"]
    open(OUTR + f'{cls}-pool.rows', 'w').write('\n'.join(hdr + [l for g in gls for l in g]) + '\n')
    tl = [H]
    for g in gls:
        for l in g[1:]:
            i = l.split(' ')[0]; x = byid[i]; tpl = SIDE.get(x[7]) or {}
            row = {h: tpl.get(h, '') for h in HC}
            row.update(spec_case='', impl_case='', id=i, size=x[8], tier='cinf', note=('case-scaleup-1111 ' + ('re-export' if x[9] == 'export' and x[6].startswith('re-export') else 'next size')).strip())
            if not tpl: row.update(family=x[5], kind='export' if x[9] == 'export' else 'join', mode='offline-batch', exploration='full')
            tl.append('\t'.join(row[h] for h in HC))
    open(OUTR + f'{cls}-pool.rows.tsv', 'w').write('\n'.join(tl) + '\n')
cand.sort(key=lambda x: (x[4], x[2], x[0]))
open(OUTP + 'candidates.tsv', 'w').write('id\thost_class\test_h\test_mem_gb\tpriority\tfamily\treason\n' + ''.join(
    f"{x[0]}\t{x[1]}\t{x[2]:.2f}\t{x[3]:.2f}\t{x[4]}\t{x[5]}\t{x[6]}\n" for x in cand))
pickle.dump(dict(cand=cand, ginfo=ginfo, dropped=dropped, kept=kept), open(SCR + 'emit.pkl', 'wb'))
c = collections.Counter((x[1], x[4]) for x in cand)
print(len(cand), 'rows;', dict(sorted(c.items())), '; dropped joins', len(dropped))
