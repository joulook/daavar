import csv,sys,re,glob,collections
R=sys.argv[1]
SL='S$_{\\mathrm{lin}}$'; SD='S$_{\\mathrm{der}}$'
def cand(d):   # a candidate directory -> its short name: source (author, language)
    m=re.match(r'chainpaxos-(paper|code)(-rust)?(-sol)?$', d)
    if not m: return d
    return f"{m.group(1)} ({'sol' if m.group(3) else 'Opus'}, {'Rust' if m.group(2) else 'Java'})"
def rust_twin(d): return d.replace('-sol','')+'-rust'+('-sol' if d.endswith('-sol') else '')
def spec_name(x): return {'hand':SL,'derived':SD,'chainderived-mixed':SD,'chainderived-local':SD,'lin':SL}.get(x) or cand('chainpaxos-'+x)
def roles(i):
    p=i.split('.'); pre=p[0]; d=p[1]; rest='.'.join(p[2:]); vs=re.search(r'\.vs-([a-z-]+)', rest); vs=vs.group(1) if vs else None
    side='spec' if rest.startswith('spec_') else 'impl'
    if pre=='C3J' and d=='pineapple-lin' and vs and vs.startswith('chainderived'): return (SL, SD, 'offline')
    if not d.startswith('chainpaxos'): return None
    if pre=='C3J' and d=='chainpaxos-derived' and side=='spec': return (SD, spec_name(vs), 'offline')
    if pre in ('C3J','XJ1') and vs: return (cand(d), spec_name(vs), 'offline')
    if pre=='XC1': return (cand(rust_twin(d)), cand(d), 'offline')
    if pre=='XC2': return (cand(d), cand(rust_twin(d)), 'offline')
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
        if not (r['program'].startswith('chainpaxos') or 'chainderived' in r['run id']): continue
        ro=roles(r['run id'])
        if not ro: continue
        if r['verdict'] in ('t/o','oom') and (r.get('timeout s') or '') != '172800': continue
        rows.append((ro, r))
best={}
for ro,r in rows:
    k=(ro[0],ro[1],r['size']+(' '+m.group(1) if (m:=re.search(r'_(mixed|local)_', r['run id'])) else ''),ro[2]); 
    if k not in best or (r.get('started') or '')>(best[k].get('started') or ''): best[k]=r
def tcell(r):
    v=r['verdict']
    if v=='t/o': return r'\faClock'
    if v=='oom': return 'oom'
    if v=='': return '--'
    return fnum(r['wall s'])
NEW='2026-11-05T00:30'
def paint(r,cells): c='blue' if (r.get('started') or '')>=NEW else ('orange' if r.get('contended')=='yes' else 'red'); return [r'\textcolor{'+c+'}{'+x+'}' for x in cells]
L=[r'\documentclass[9pt]{article}',r'\usepackage[a4paper,landscape,margin=12mm]{geometry}',r'\usepackage{booktabs,longtable,amsmath,xcolor,fontawesome5}',r'\usepackage[T1]{fontenc}',r'\setlength{\tabcolsep}{4pt}',r'\begin{document}',r'\section*{The ChainPaxos study (USENIX ATC 2022): two specifications, six context-free candidates from the paper and from the Java implementation}',
   r"S$_{\mathrm{lin}}$ = the hand-written linearizable register specification reused from the Pineapple study; S$_{\mathrm{der}}$ = the specification derived from the ChainPaxos paper by a context-free agent (Opus), reviewed by Astra (too loose for the local-read variant under the interface's bounded no-op rule). The candidates were written by context-free agents in Rust (TraceForge) and in Java (JMC), from the paper (sections 3--4) and from the public Java implementation (chainpaxos, frontend, app packages), by Opus and by Codex sol; none saw a specification. A row is one check at $q$ commands per client (two clients at the head and the tail of a fixed chain n0--n1--n2, n0 the leader, no failure, fifo), variant mixed (reads ordered through the chain) or local (the paper's local reads, the no-op timer as a bounded nondeterministic choice), offline (exports and merge join). $G$ as in the paper, $G_x$ = uncovered graphs; T = critical path, M = peak memory (MiB).",r'\medskip']
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
open(R+'/plan/twin-tables/CHAINPAXOS-STUDY.tex','w').write('\n'.join(L)+'\n'); print('study written:', len(best), 'checks')
