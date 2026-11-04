import csv,sys,re,glob,collections
R=sys.argv[1]
NAME={'omnipaxos-paper':'paper (Opus)','omnipaxos-paper-sol':'paper (sol)','omnipaxos-rust':'rust-src (Opus)','omnipaxos-rust-sol':'rust-src (sol)','omnipaxos-derived-impl':'S$_{\\mathrm{paper}}$-impl (Opus)','omnipaxos-derived-sol':'S$_{\\mathrm{paper}}$-impl (sol)','omnipaxos-spec':'S$_{\\mathrm{raft}}$-impl (Opus)','omnipaxos-spec-sol':'S$_{\\mathrm{raft}}$-impl (sol)'}
def spec_name(x): return {'hand':'S$_{\\mathrm{raft}}$','derived':'S$_{\\mathrm{paper}}$','paper':NAME['omnipaxos-paper'],'spec':NAME['omnipaxos-spec'],'paper-sol':NAME['omnipaxos-paper-sol'],'spec-sol':NAME['omnipaxos-spec-sol']}.get(x, x)
def roles(i):
    p=i.split('.'); pre=p[0]; d=p[1]; rest='.'.join(p[2:]); vs=re.search(r'\.vs-([a-z-]+)', rest); vs=vs.group(1) if vs else None
    side='spec' if rest.startswith('spec_') else 'impl'
    if pre=='C3J' and d=='omnipaxos' and side=='spec': return ('S$_{\\mathrm{raft}}$', spec_name(vs), 'offline')
    if pre=='C3J' and d=='omnipaxos-derived': return ('S$_{\\mathrm{paper}}$', spec_name(vs), 'offline')
    if pre=='C3J' and d=='omnipaxos': return ('Rust OmniPaxos', spec_name(vs) if vs else 'S$_{\\mathrm{raft}}$', 'offline')
    if pre=='C1' and d=='omnipaxos': return ('Rust OmniPaxos', 'S$_{\\mathrm{raft}}$', 'in-process')
    if pre=='XJ1': return (NAME.get(d,d), spec_name(vs), 'offline')
    if pre=='XC1' and vs: return (NAME.get(d,d), spec_name(vs), 'offline')
    if pre=='XC1': return ('Rust OmniPaxos', NAME.get(d,d), 'offline')
    if pre=='XC2': return (NAME.get(d,d), 'Rust OmniPaxos', 'offline')
    if pre=='XCON2': return ('Rust OmniPaxos', NAME.get(d,d), 'online')
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
        if not r['program'].startswith('omnipaxos') or r['variant']=='reorder': continue
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
NEW='2026-11-03T23:30'
def paint(r,cells): c='blue' if (r.get('started') or '')>=NEW else ('orange' if r.get('contended')=='yes' else 'red'); return [r'\textcolor{'+c+'}{'+x+'}' for x in cells]
L=[r'\documentclass[9pt]{article}',r'\usepackage[a4paper,landscape,margin=12mm]{geometry}',r'\usepackage{booktabs,longtable,amsmath,xcolor,fontawesome5}',r'\usepackage[T1]{fontenc}',r'\setlength{\tabcolsep}{4pt}',r'\begin{document}',r'\section*{The OmniPaxos study: two specifications, one Rust library, eight context-free Java candidates}',
   r"S$_{\mathrm{raft}}$ = the client-visible content of Ongaro's TLA$^+$ specification of Raft (one decided log, deliveries in log order, a proposal appended or lost); S$_{\mathrm{paper}}$ = the specification derived from the Omni-Paxos paper by a context-free agent (version 3: decides each proposal as it arrives). Rust OmniPaxos = the unmodified library under a harness. The Java candidates were written by context-free agents from the paper, from the Rust library's source, from S$_{\mathrm{paper}}$ and from S$_{\mathrm{raft}}$, by Opus and by Codex sol. A row is one check: the implementation side against the specification side (which may itself be a specification or another implementation), at a size (e, t, cand), offline (exports and merge join), online (the Rust side in-process against the other side's export) or in-process. $G$ as in the paper, $G_x$ = uncovered graphs; T = critical path, M = peak memory (MiB). Colours as in the other tables (blue new, orange contended, red earlier).",r'\medskip']
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
open(R+'/plan/twin-tables/OMNIPAXOS-STUDY.tex','w').write('\n'.join(L)+'\n'); print('study written:', len(best), 'checks')
