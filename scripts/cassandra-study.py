import csv,sys,re,glob,collections
R=sys.argv[1]
SL='S$_{\\mathrm{cas}}$'; SD='S$_{\\mathrm{der}}$'
def cand(d):
    if d=='cassandra-paxos-v1': return 'Java transliteration (ref.)'
    m=re.match(r'cassandra-paxos-v1(-rust)?(-sol)?$', d)
    if not m: return d
    return f"port ({'sol' if m.group(2) else 'Opus'}, {'Rust' if m.group(1) else 'Java'})"
def spec_name(x): return {'hand':SL,'derived':SD,'cas':SL,'rust':cand('cassandra-paxos-v1-rust'),'rust-sol':cand('cassandra-paxos-v1-rust-sol')}.get(x) or x
def roles(i):
    p=i.split('.'); pre=p[0]; d=p[1]; rest='.'.join(p[2:]); vs=re.search(r'\.vs-([a-z-]+)', rest); vs=vs.group(1) if vs else None
    side='spec' if rest.startswith('spec_') else 'impl'
    if not d.startswith('cassandra'): return None
    if pre=='C3J' and d=='cassandra-cas' and side=='spec': return (SL, spec_name(vs), 'offline')
    if pre=='C3J' and d=='cassandra-derived' and side=='spec': return (SD, spec_name(vs), 'offline')
    if pre in ('C3J','XJ1') and vs: return (cand(d), spec_name(vs), 'offline')
    if pre=='XC1': return (spec_name(vs) if vs else cand('cassandra-paxos-v1-rust'+('-sol' if d.endswith('-sol') else '')), cand(d), 'offline')
    if pre=='XC2': return (cand(d), spec_name(vs) if vs else cand('cassandra-paxos-v1-rust'+('-sol' if d.endswith('-sol') else '')), 'offline')
    return None
def fnum(x):
    try: v=float(x); return str(int(v)) if v==int(v) else '%.1f' % v
    except Exception: return '--'
def mib(x):   # the CSV's peak MB is already KiB/1024, i.e. MiB (audit 2026-11-04: no second division)
    try: return str(round(float(x)))
    except Exception: return '--'
rows=[]
for f in glob.glob(R+'/PLDI/tex/tables/campaign-*.csv'):
    for r in csv.DictReader(open(f)):
        if not r['program'].startswith('cassandra'): continue
        ro=roles(r['run id'])
        if not ro: continue
        if r['verdict'] in ('t/o','oom') and (r.get('timeout s') or '') != '172800': continue
        rows.append((ro, r))
best={}
for ro,r in rows:
    k=(ro[0],ro[1],r['size']+(' '+m.group(1) if (m:=re.search(r'_(v1nolin|v1)_', r['run id'])) else ''),ro[2]); 
    if k not in best or (r.get('started') or '')>(best[k].get('started') or ''): best[k]=r
def tcell(r):
    v=r['verdict']
    if v=='t/o': return r'\faClock'
    if v=='oom': return 'oom'
    if v=='': return '--'
    return fnum(r['wall s'])
NEW='2026-11-04T20:00'
def paint(r,cells): c='blue' if (r.get('started') or '')>=NEW else ('orange' if r.get('contended')=='yes' else 'red'); return [r'\textcolor{'+c+'}{'+x+'}' for x in cells]
L=[r'\documentclass[9pt]{article}',r'\usepackage[a4paper,landscape,margin=12mm]{geometry}',r'\usepackage{booktabs,longtable,amsmath,xcolor,fontawesome5}',r'\usepackage[T1]{fontenc}',r'\setlength{\tabcolsep}{4pt}',r'\begin{document}',r'\section*{The Cassandra study: lightweight transactions (Paxos v1) of Apache Cassandra 4.1, two specifications, a transliteration and three context-free ports}',
   r"S$_{\mathrm{cas}}$ = the hand-written specification of a linearizable compare-and-set register with unknown outcomes (a request may be answered Failed, and a failed Cas may still take effect later); S$_{\mathrm{der}}$ = the specification derived by a context-free agent (Opus) from Cassandra's own design document (service/paxos/Paxos.md), reviewed by Astra (it admits a decided Cas answered as not applied). The Java transliteration keeps Cassandra's classes and methods on JMC's API (the reference, not scored); the ports were written by context-free agents from the same sources in Rust (TraceForge) and Java (JMC), by Opus and by Codex sol. A row is one check at $q$ operations per client (two clients, three replicas, no failure, fifo), variant v1 or v1nolin, offline (exports and merge join). $G$ as in the paper, $G_x$ = uncovered graphs; T = critical path, M = peak memory (MiB).",r'\medskip']
for viol,title in ((False,'Conforming pairs'),(True,'Violating pairs')):
    sel=[(k,r) for k,r in best.items() if r['verdict'] in ('conforms','violation') and (r['verdict']=='violation')==viol]
    sel.sort(key=lambda kr:(kr[0][0],kr[0][1],kr[0][2]))
    hdr=r'implementation side & specification side & size & kind & $G_{\mathit{impl}}$ & $G_{\mathit{spec}}$'+(r' & $G_x$' if viol else '')+r' & T & M \\'
    L+=[r'\begin{longtable}{@{}lllr'+('rrr' if viol else 'rr')+r'rr@{}}', r'\caption{'+title+' ('+str(len(sel))+r' checks).}\\', r'\toprule', hdr, r'\midrule', r'\endfirsthead', r'\toprule', hdr, r'\midrule', r'\endhead', r'\bottomrule', r'\endfoot']
    for k,r in sel:
        cells=[k[0],k[1],k[2],k[3],r['impl graphs'] or '--',r['spec graphs'] or '--']+([r.get('uncovered') or '--'] if viol else [])+[tcell(r), mib(r['peak MB'])]
        L.append(' & '.join(paint(r,cells))+r' \\')
    L+=[r'\end{longtable}']
und=[(k,r) for k,r in best.items() if r['verdict'] not in ('conforms','violation')]
if und:
    hdr=r'implementation side & specification side & size & kind & outcome \\'
    L+=[r'\begin{longtable}{@{}lllrl@{}}', r'\caption{Checks without a verdict ('+str(len(und))+r': time or memory limit, or no verdict).}\\', r'\toprule', hdr, r'\midrule', r'\endfirsthead', r'\toprule', hdr, r'\midrule', r'\endhead', r'\bottomrule', r'\endfoot']
    for k,r in sorted(und, key=lambda kr:kr[0]): L.append(' & '.join(paint(r,[k[0],k[1],k[2],k[3],tcell(r)]))+r' \\')
    L+=[r'\end{longtable}']
L.append(r'\end{document}')
open(R+'/plan/twin-tables/CASSANDRA-STUDY.tex','w').write('\n'.join(L)+'\n'); print('study written:', len(best), 'checks')
