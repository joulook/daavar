import csv,sys,re,glob,collections
R=sys.argv[1]
SL='S$_{\\mathrm{lin}}$'; SD='S$_{\\mathrm{der}}$'
def cand(d):   # a candidate directory -> its short name: source (author, language)
    m=re.match(r'pineapple-(paper|code)(-rust)?(-sol)?$', d)
    if not m: return d
    return f"{m.group(1)} ({'sol' if m.group(3) else 'Opus'}, {'Rust' if m.group(2) else 'Java'})"
def rust_twin(d): return d.replace('-sol','')+'-rust'+('-sol' if d.endswith('-sol') else '')
def spec_name(x): return {'hand':SL,'derived':SD,'lin':SL}.get(x) or cand('pineapple-'+x)
def roles(i):
    p=i.split('.'); pre=p[0]; d=p[1]; rest='.'.join(p[2:]); vs=re.search(r'\.vs-([a-z-]+)', rest); vs=vs.group(1) if vs else None
    side='spec' if rest.startswith('spec_') else 'impl'
    if not d.startswith('pineapple'): return None
    if pre=='C3J' and d=='pineapple-lin' and side=='spec': return (SL, spec_name(vs), 'offline')
    if pre=='C3J' and d=='pineapple-derived' and side=='spec': return (SD, spec_name(vs), 'offline')
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
        if not r['program'].startswith('pineapple'): continue
        ro=roles(r['run id'])
        if not ro: continue
        if r['verdict'] in ('t/o','oom') and (r.get('timeout s') or '') != '172800': continue
        rows.append((ro, r))
best={}
for ro,r in rows:
    k=(ro[0],ro[1],r['size'],ro[2]); 
    if k not in best or (r.get('started') or '')>(best[k].get('started') or ''): best[k]=r
def tcell(r):
    v=r['verdict']
    if v=='t/o': return r'\faClock'
    if v=='oom': return 'oom'
    if v=='': return '--'
    return fnum(r['wall s'])
NEW='2026-11-04T18:00'
def paint(r,cells): c='blue' if (r.get('started') or '')>=NEW else ('orange' if r.get('contended')=='yes' else 'red'); return [r'\textcolor{'+c+'}{'+x+'}' for x in cells]
L=[r'\documentclass[9pt]{article}',r'\usepackage[a4paper,landscape,margin=12mm]{geometry}',r'\usepackage{booktabs,longtable,amsmath,xcolor,fontawesome5}',r'\usepackage[T1]{fontenc}',r'\setlength{\tabcolsep}{4pt}',r'\begin{document}',r'\section*{The Pineapple study (NSDI 2025): two specifications, eight context-free candidates from the paper and from the public Go prototype}',
   r"S$_{\mathrm{lin}}$ = the hand-written specification of Pineapple's single-key interface (one object applying Write, Read and the one-shot Rmw $f(v)=v+10$ in arrival order and answering the client named in the request); S$_{\mathrm{der}}$ = the specification derived from the paper by a context-free agent (Opus), reviewed by Astra (faithful). The candidates were written by context-free agents in Rust (TraceForge) and in Java (JMC), from the paper (sections 2.2, 3, Appendix A) and from the public Go prototype's sources (pineapple.go, pineappleproto.go, genericsmr.go), by Opus and by Codex sol; none saw a specification. A row is one check: the implementation side against the specification side (which may itself be a specification or another candidate), at $q$ operations per client (two clients, three nodes, stable leader, fifo), offline (exports and merge join). $G$ as in the paper, $G_x$ = uncovered graphs; T = critical path, M = peak memory (MiB). Colours as in the other tables (blue = this study's runs).",r'\medskip']
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
open(R+'/plan/twin-tables/PINEAPPLE-STUDY.tex','w').write('\n'.join(L)+'\n'); print('study written:', len(best), 'checks')
