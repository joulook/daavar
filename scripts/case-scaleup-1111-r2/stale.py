#!/usr/bin/env python3
"""case-scaleup-1111 r2, Part A audit: every 'exists (<id>)' note of round 1 (tmp/case-scaleup-1111/cands.pkl), classified against
the r2 load (brain-data, rows files, live CASE rows, round-1 rows as pending): measured / running / pending / stale rows-only /
failed (repeating or not). Writes $SCR/stale.tsv."""
import pickle, re, collections, sys
SCR = '/local/home/mkhoshechin/tmp/case-scaleup-1111-r2/'
sys.argv = ['gen']; exec(open('/local/home/mkhoshechin/Documents/github-jmc/daavar/scripts/case-scaleup-1111-r2/gen.py').read().split('# ======== round 2 ========')[0])
exec(open('/local/home/mkhoshechin/Documents/github-jmc/daavar/scripts/case-scaleup-1111-r2/gen.py').read().split('# ======== round 2 ========')[1].split('JF = collections')[0])
X1 = pickle.load(open('/local/home/mkhoshechin/tmp/case-scaleup-1111/cands.pkl', 'rb'))
out = []
for fam, ns in X1['notes'].items():
    for n in ns:
        m = re.match(r'(\S+) exists \((\S+)\)', n)
        if not m: continue
        size, i = m.groups(); hit = sorted(B2.get(base2(i), ()))
        ok = [h for h in hit if recok(h)]; lv = [h for h in hit if h in LIVE]
        recs = [(h, x['stop_reason'], x.get('exit_code', ''), x['node'], fail_repeats(x)) for h in hit for x in R.get(h, [])]
        if ok: cl = 'measured'; det = ok[0]
        elif lv: cl = LIVE[lv[0]][0]; det = f'{lv[0]} on {LIVE[lv[0]][1]}'
        elif any(r[4] for r in recs): cl = 'failed-repeats'; det = '; '.join(f'{r[0]} {r[1]} on {r[3]} ({r[4]})' for r in recs if r[4])[:300]
        elif recs: cl = 'failed-other'; det = '; '.join(f'{r[0]} {r[1]} exit {r[2]} on {r[3]}' for r in recs)[:300]
        else: cl = 'stale'; det = 'rows files: ' + ', '.join(sorted({x[0] for h in hit for x in ROWS.get(h, [])}))
        out.append((fam, size, i, cl, det))
open(SCR + 'stale.tsv', 'w').write('family\tsize\tid\tclass\tdetail\n' + ''.join('\t'.join(o) + '\n' for o in sorted(out)))
print(len(out), collections.Counter(o[3] for o in out))
