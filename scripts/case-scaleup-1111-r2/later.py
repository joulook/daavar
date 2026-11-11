#!/usr/bin/env python3
"""case-scaleup-1111 r2, step 5 (coordinator 2026-11-11, duplicates accepted when they enable a new join): the 'later' joins of
gen.py placed on the host H of the export they wait for (round-1 nodes/<host>.rows, or running/pending CASE rows of tmp/sweep-1111),
needok on that export; each other export the join reads is reused when it has an ok record on H or is pending/running/placed on H,
else re-exported on H under a fresh .cinf.rN id. Writes cluster/campaign/case-scaleup-1111/r2/later/<host>.rows(.rows.tsv) and SUMMARY.md.
Estimates as gen.py (planning numbers, R10)."""
import collections, glob, os, pickle, re, sys
S2 = '/local/home/mkhoshechin/Documents/github-jmc/daavar/scripts/case-scaleup-1111-r2/gen.py'
src = open(S2).read(); exec(src.split('# ======== round 2 ========')[0]); exec(src.split('# ======== round 2 ========')[1].split('JF = collections')[0])
SW = '/local/home/mkhoshechin/tmp/sweep-1111/'
OUT = G + 'daavar/cluster/campaign/case-scaleup-1111/r2/later/'
X = pickle.load(open(SCR + 'cands.pkl', 'rb')); E2 = pickle.load(open(SCR + 'emit.pkl', 'rb'))
HOST = collections.defaultdict(set)   # id -> hosts where it is placed, running or pending
for fn in glob.glob(G + 'daavar/cluster/campaign/case-scaleup-1111/nodes/*.rows'):
    h = os.path.basename(fn)[:-5]
    for l in open(fn):
        if l.strip() and not l.startswith('#'): HOST[l.split(' ', 1)[0]].add(h)
for l in open(SW + 'running-live.tsv'):
    t = l.rstrip('\n').split('\t')
    if len(t) > 3 and t[2] == 'CASE': HOST[t[3]].add(t[1])
for l in open(SW + 'pending-dedup.tsv'):
    t = l.rstrip('\n').split('\t')
    if len(t) > 4 and t[2] == 'CASE': HOST[t[4]].add(t[1])
ONH = collections.defaultdict(list)   # (host, ekey, nums) -> ids usable on that host (ok record there, or placed/live there)
for i in ALL:
    if not EXRE.match(i): continue
    hs = set(HOST.get(i, ())) | {r['node'] for r in R.get(i, []) if r['stop_reason'] == 'ok'}
    for h in hs: ONH[(h, ekey(i), nums(i))].append(i)
def hclass(h): return 'brain' if h.startswith('brain') else ('volta' if h.startswith('volta') else 'pinky')
CLS = dict(brain=dict(mem=250, cap=230, heap='12g', hg=12, f=1.0, lim=36), volta=dict(mem=200, cap=180, heap='16g', hg=16, f=2.0, lim=36), pinky=dict(mem=80, cap=70, heap='8g', hg=8, f=2.0, lim=36))
USED = {x[0] for x in E2['cand']}
def fresh(i):
    b, r = i, 2
    while i in ALL or i in USED: i = f'{b}.r{r}'; r += 1
    USED.add(i); return i
def est(e, P, C, k, s_):
    ek = ekey(e); done = ex_best(ek, C)
    if done: return done[0], done[1], f'measured at C: {done[2]} {done[0]:.0f}s {done[1]:.0f}MB on {done[3]}'
    sp = ex_best(ek, P)
    if not sp: return None
    sr = step_ratio(ek, P, k)
    if not sr:
        alt = [a for a in (step_ratio(k2, P, k) for k2 in list(EX) if k2[1].split('-')[0] == ek[1].split('-')[0] and k2[0] == ek[0] and k2 != ek) if a]
        if alt: sr = max(alt)
    if not sr and sp[0] < 60:
        alt = [a for a in (step_ratio(ek, P, k3) for k3 in range(len(P)) if k3 != k) if a]
        if alt: sr = max(alt)
    if not sr: return None
    g, mr, stp, acc = sr; g = g ** s_; mr = mr ** s_
    jmc = e.startswith(('XCE', 'XJE'))
    return max(sp[0], 1.0) * g, sp[1] * (g ** 0.6 if (jmc and sp[1] < 500) else min(g, max(mr, 1.0))), f'{sp[2]} {sp[0]:.0f}s {sp[1]:.0f}MB x g={g:.2f} (step {stp[0]} -> {stp[1]})'
H_ = "id\ttable\tfamily\tsize\tmodel\tvariant\tengine\tthreads\texpected_verdict\tnote\ttier\tkind\tspec_tool\timpl_tool\tspec_lang\timpl_lang\tmode\texploration\tbenchmark\tspec_case\timpl_case"
HC = H_.split('\t')
rows = collections.defaultdict(list); side = collections.defaultdict(list); hours = collections.defaultdict(float); left = []
NEWEX = {}   # (host, ekey, C) -> new export id placed in this step
placed_j = set()
sz = lambda c: ','.join(map(str, c))
def addside(h, i, tid, size, note, kind):
    tpl = SIDE.get(tid) or {}; row = {x: tpl.get(x, '') for x in HC}
    row.update(spec_case='', impl_case='', id=i, size=size, tier='cinf', note=note)
    if not tpl: row.update(kind=kind, mode='offline-batch', exploration='full')
    side[h].append('\t'.join(row[x] for x in HC))
for l in sorted(X['later'], key=lambda l: (l['part'], l['fam'], l['C'])):
    tag = f"{l['part']} {l['fam']} {sz(l['P'])} -> {sz(l['C'])}"
    m = re.match(r'(running|pending|pending-r1) export (\S+) at (\S+)', l['why'])
    if not m: left.append((tag, 'waits for an export proposed in round 2 (' + l['why'] + '), not placed on a host yet')); continue
    W = m.group(2); hs = sorted(HOST.get(W, ()))
    if not hs: left.append((tag, f'awaited export {W} has no known host')); continue
    h = hs[0]; K = CLS[hclass(h)]
    jid = l['jid']; cmd = ROWS[jid][0][3]; r = recok(jid)
    exs = [e for e in EXIDRE.findall(cmd) if EXRE.match(e) and e.count('.') >= 2]
    P, C = l['P'], l['C']; k = next(j for j in range(len(P)) if P[j] != C[j]); s_ = C[k] - P[k]
    plan = []; bad = None; gmax = 1.0
    for e in exs:
        ek = ekey(e)
        use = sorted(ONH.get((h, ek, C), []))
        if (h, ek, C) in NEWEX: use = [NEWEX[(h, ek, C)]]
        if use:
            u = use[0]; plan.append(dict(old=e, new=u, row=None)); 
            if not ex_best(ek, C):
                g_ = est(e, P, C, k, s_)
                if g_: gmax = max(gmax, g_[0] / max(1.0, (ex_best(ek, P) or (1.0,))[0]))
            continue
        tid, tcmd = export_template(e); ncmd = sub_cmd(tcmd, C) if tcmd else None
        es = est(e, P, C, k, s_)
        if not ncmd or not es: bad = f'cannot build or estimate the re-export of {e} at {sz(C)}'; break
        t, mm, basis = es
        jmc = e.startswith(('XCE', 'XJE'))
        if t / 3600 * K['f'] > K['lim'] or mm / 1024 > K['cap'] or (jmc and mm / 1024 > 17 * K['hg']):
            bad = f're-export of {e} at {sz(C)} estimated {t / 3600 * K["f"]:.1f} h, {mm / 1024:.1f} GB on {hclass(h)} {h}: over the class limit'; break
        if not ex_best(ek, C): gmax = max(gmax, t / max(1.0, ex_best(ek, P)[0]))
        plan.append(dict(old=e, new=None, row=(ek, ncmd, t, mm, basis, jmc, tid)))
    if bad: left.append((tag, bad)); continue
    jt = (f(r['wall_ms']) or 1000) / 1000 * node_factor(r['node']) * gmax; jm = (f(r['peak_rss_kb']) or 0) / 1024 * gmax
    if jt / 3600 * K['f'] > K['lim'] or jm / 1024 > K['cap']: left.append((tag, f'join estimated {jt / 3600 * K["f"]:.1f} h, {jm / 1024:.1f} GB on {h}: over the class limit')); continue
    jn = fresh(new_join_id(jid, C))
    if (h, jn) in placed_j: continue
    for p in plan:
        if p['row']:
            ek, ncmd, t, mm, basis, jmc, tid = p['row']
            b0 = new_export_id(p['old'], C); n0 = 2
            while f'{b0}.r{n0}' in ALL or f'{b0}.r{n0}' in USED: n0 += 1
            nid = f'{b0}.r{n0}'; USED.add(nid)
            if jmc: ncmd = f"env JAVA_TOOL_OPTIONS=-Xmx{K['heap']} " + ncmd
            rows[h].append(f"{nid} {K['mem']} 8640000 {ncmd}"); hours[h] += t / 3600 * K['f']
            addside(h, nid, tid, sz(C), f're-export on {h} for {jn} (case-scaleup-1111 r2 later); est {t / 3600 * K["f"]:.2f} h {mm / 1024:.1f} GB; basis {basis}', 'export')
            NEWEX[(h, ek, C)] = nid; p['new'] = nid
    jc = re.sub(r'^needok:\S+ ', '', cmd).replace('"$EVAL_OUT"/imports/', '"$EVAL_OUT"/')
    jc = re.sub(r'(--timeout|timeout) 172800', r'\1 8640000', jc)
    for p in plan: jc = jc.replace(f'"$EVAL_OUT"/{p["old"]}/export.jsonl', f'"$EVAL_OUT"/{p["new"]}/export.jsonl')
    need = [W] + [p['new'] for p in plan if p['new'] != W]
    rows[h].append(f"{jn} {K['mem']} 8640000 needok:{','.join(need)} {jc}"); hours[h] += jt / 3600 * K['f']; placed_j.add((h, jn))
    addside(h, jn, jid, sz(C), f'case-scaleup-1111 r2 later, part {l["part"]}: waits for {W} on {h}; {l["fam"]} {sz(P)} -> {sz(C)}; est {jt / 3600 * K["f"]:.2f} h' + ('; re-export' if any(p['row'] for p in plan) else ''), 'join')
for fn in glob.glob(OUT + '*.rows*'): os.remove(fn)
for h in sorted(rows):
    K = CLS[hclass(h)]
    hdr = [f"# case-scaleup-1111 r2 later rows for {h} (coordinator 2026-11-11: duplicate exports accepted when they enable a new join); generated by scripts/case-scaleup-1111-r2/later.py; {len(rows[h])} rows, est {hours[h]:.1f} h on {hclass(h)} (planning only, R10); tier cinf, mem {K['mem']}; each join has needok on the export it waits for (placed or running on {h}); re-exports precede their joins.",
           f"# heap -Xmx{K['heap']} for JMC exports; at most one JMC export at a time on a pinky or volta, three on a brain"]
    open(OUT + f'{h}.rows', 'w').write('\n'.join(hdr + rows[h]) + '\n')
    open(OUT + f'{h}.rows.tsv', 'w').write('\n'.join([H_] + side[h]) + '\n')
L = ['# r2 later rows: summary (generated by daavar/scripts/case-scaleup-1111-r2/later.py; estimates are planning numbers, R10)', '',
     f'Input: the {len(X["later"])} "later" sizes of ../PLAN.md. Placed: {sum(1 for h in rows for x in rows[h] if x.split(" ")[0].endswith((".m",)) or ".m.r" in x.split(" ")[0])} joins on {len(rows)} hosts; left out: {len(left)}.', '',
     'Rules: a join goes to the host H of the export it waits for (round-1 nodes/<host>.rows, else running-live.tsv/pending-dedup.tsv CASE rows; the first host alphabetically when several), needok on that export and on the join\'s inputs; an input with an ok record on H or placed/running/pending on H is reused, else re-exported on H (fresh id, sidecar note "re-export"). Limits: est <= 36 h on the class (volta/pinky time = brain x 2), peak <= 230/180/70 GB, JMC export <= 17 x heap. When the awaited export is at a smaller size than the join, the join\'s own export at its size is a re-export on H estimated from the measured size P (step ratio, as gen.py); the needok on the smaller export only gates it.', '',
     'Not modelled: the slot count of a host (brain08/12/13/14/15 four slots, brain01-05 eight) and the one-JMC-export-at-a-time rule on pinky/volta are placement constraints for the dispatcher; the hours below are the sum of the rows, not wall clock.', '',
     '| host | class | rows | joins | re-exports | est h |', '|---|---|---|---|---|---|']
for h in sorted(rows):
    nj = sum(1 for x in rows[h] if ' needok:' in x and '.m' in x.split(' ')[0]); L.append(f'| {h} | {hclass(h)} | {len(rows[h])} | {nj} | {len(rows[h]) - nj} | {hours[h]:.1f} |')
L += ['', f'Total est {sum(hours.values()):.1f} h.', '', '## Left out', '']
for t, w in left: L.append(f'- {t}: {w}')
open(OUT + 'SUMMARY.md', 'w').write('\n'.join(L) + '\n')
print(len(rows), 'hosts', sum(len(v) for v in rows.values()), 'rows', f'{sum(hours.values()):.1f} h', len(left), 'left', collections.Counter(w.split(' ')[0] for t, w in left))
