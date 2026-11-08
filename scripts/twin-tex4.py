import csv,sys,re,collections
R=sys.argv[1]; MODE=sys.argv[2] if len(sys.argv)>2 else 'twin'
# (offline kind, online kind, title): the two kinds that check the same side against the same export
PAIRS={'twin':[('campaign-twin-offline-java-spec','campaign-twin-online-rust-impl','Twins: Rust implementation checked against the Java twin\'s export'),
               ('campaign-twin-offline-rust-spec','campaign-twin-online-java-impl','Twins: Java twin checked against the Rust implementation\'s export'),
               ('campaign-cftwin-offline-java-spec','campaign-cftwin-online-rust-impl','Context-free twins: Rust implementation checked against the Java port\'s export'),
               ('campaign-cftwin-offline-rust-spec','campaign-cftwin-online-java-impl','Context-free twins: Java port checked against the Rust implementation\'s export')],
       'cross':[('campaign-offline-rust-spec','campaign-online-java-impl','Cross-language: Java implementation against the Rust specification\'s export'),
                ('campaign-offline-java-spec','campaign-online-rust-impl','Cross-language: Rust implementation against the Java specification\'s export')],
       'realsw':[('campaign-offline-tf','campaign-stateful','Real software and agentic: implementation against the specification (offline, and in-process)'),
                 ('campaign-offline-java-spec','campaign-online-rust-impl','Real software and agentic: Rust implementation against the Java specification\'s export'),
                 ('campaign-offline-rust-spec','campaign-online-java-impl','Real software and agentic: Java implementation against the Rust specification\'s export')]}[MODE]
REAL={'raft','omnipaxos','omnipaxos-derived','omnipaxos-paper','omnipaxos-spec','omnipaxos-paper-sol','omnipaxos-rust-sol','omnipaxos-derived-sol','omnipaxos-spec-sol','hermes','vrr','ricart-agrawala','chain-replication','caspaxos','flexible-paxos','ben-or'}
NAMES={("2pc","early-abort"):"2pc-ea",("2pc-ab","off-by-one"):"2pc-ab-off1",("2pc-ds","skip-ack-loop"):"2pc-ds-sal",("2pc-lb","counting-bug"):"2pc-lb-cb",("2pc-le","ring-conf"):"2pc-le",("2pc-le","ring-split"):"2pc-le-split",("2pc-tr","ignore-abort-vote"):"2pc-tr-iav",("bcast","decide-early"):"bcast-de",("chan","no-seqno"):"chan-ns",("ewd998","no-black-on-receive"):"ewd998-nb",("lb","rr-a4b"):"lb",("lb","rr-a4a"):"lb-strict",("lock","fifo"):"lock",("lock","wrong-round"):"lock-wr",("pbkv","pb"):"pbkv",("replog-co","early-reply"):"replog-co-er",("ring-el","no-compare"):"ring-el-nc",("zab","issue 1"):"zab-issue1",
       ("paxos","skip-2a-check"):"paxos-s2a",("paxos","half-majority"):"paxos-hm",("paxos","ignore-max-vote"):"paxos-imv",("paxos","1b-none"):"paxos-1b",("abd","read-no-writeback"):"abd-rnw",("nbuyer","no-subtract"):"nbuyer-ns",("bank","ordered"):"bank",("bank","unordered"):"bank-un",("eokv","exactly-once"):"eokv",("eokv","no-request-id"):"eokv-nri",("fencing","fenced"):"fencing",("fencing","stale-frontend"):"fencing-sf",("sikv","serializable"):"sikv",("sikv","si"):"sikv-si",("rpc","dedup"):"rpc",("rpc","no-dedup"):"rpc-nd",("memcached","lost-set"):"memcached-ls",("memcached","shared-bucket"):"memcached-sb",("ironlock","double-hold"):"ironlock-dh",("ironlock","grant-without-token"):"ironlock-gwt",("replog","asyn-fault"):"replog-af",("calc","batched-swap"):"calc-bs",("calc","memo-ignore-args"):"calc-mia",("calc","delegating-prev"):"calc-dp",("pbkv","early-ack"):"pbkv-ea",("pbkv","stale-get"):"pbkv-sg",("pbkv","silent-put"):"pbkv-sp",("pbkv","pb-br"):"pbkv-br",("pbkv","control-early-ack-fwd"):"pbkv-ceaf",("pbkv","control-early-ack-primary"):"pbkv-ceap",("lock","double-grant"):"lock-dg",("lock","never-grant"):"lock-ng",("lock","control-lifo"):"lock-lifo",("2pc","eager"):"2pc-eager",("2pc","silent"):"2pc-silent",("2pc","control-reverse"):"2pc-rev",("lb","drop-a4a"):"lb-drop",("lb","hash-drop-a4a"):"lb-hdrop",("lb","hash-a4a"):"lb-hash",("lb","pair-swap-hash-a4a"):"lb-psh",("lb","pair-swap-rr-a4a"):"lb-psrr",("lb-cap","l2-stale"):"lb-cap-stale",("prodcons","impl"):"prodcons"}
def short(prog,var):
    if (prog,var) in NAMES: return NAMES[(prog,var)]
    if var in ("correct","impl",prog,""): return prog
    w=[x for x in re.split(r"[-_ ]",var) if x]; return prog+"-"+("".join(x[0] for x in w) if len(w)>1 else var[:6])
def esc(s): return s.replace('_','\\_').replace('%','\\%').replace('&','\\&').replace('#','\\#').replace('$','\\$')
def fnum(x):   # seconds: 1 decimal from 10 s, 2 decimals below, 3 below 0.1 s; a true zero prints as 0 (never a rounded one)
    try: v=float(x)
    except Exception: return '--'
    if v==0: return '$<$0.01'   # /usr/bin/time records at 10 ms resolution: a 0 is a run below it, not a missing measurement
    from decimal import Decimal, ROUND_HALF_UP   # half up on the value's text: 287.25 prints 287.3, not 287.2 (audit 1108-1703)
    d = lambda nd: str(Decimal(str(x).strip()).quantize(Decimal(1).scaleb(-nd), rounding=ROUND_HALF_UP))
    if v>=10: return d(1)
    if v>=0.1: return d(2)
    return d(3)
def mib(x):   # the CSV's peak MB is already KiB/1024, i.e. MiB (audit 2026-11-04: no second division)
    try: return str(round(float(x)))
    except Exception: return '--'
def gi(x):
    try: return int(float(x))
    except Exception: return -1
AGENTIC={'hermes','vrr','ricart-agrawala','chain-replication','caspaxos','flexible-paxos','ben-or'}
ONAME={'omnipaxos-paper':'OmniPaxos paper port (Opus, Java)','omnipaxos-paper-sol':'OmniPaxos paper port (Codex, Java)','omnipaxos-rust':'OmniPaxos source port (Opus, Java)','omnipaxos-rust-sol':'OmniPaxos source port (Codex, Java)','omnipaxos-derived-impl':r'S$_{\mathrm{paper}}$ port (Opus, Java)','omnipaxos-derived-sol':r'S$_{\mathrm{paper}}$ port (Codex, Java)','omnipaxos-spec':r'S$_{\mathrm{raft}}$ port (Opus, Java)','omnipaxos-spec-sol':r'S$_{\mathrm{raft}}$ port (Codex, Java)','omnipaxos-derived':r'S$_{\mathrm{paper}}$ (derived, Rust)'}
def spec_of(P,r):   # the specification a real-software row is checked against (the vs- word of the run id, else the hand-written one)
    if P.startswith('omnipaxos') or P=='raft':
        vs=re.search(r'\.vs-([a-z-]+)', r['run id']); w=vs.group(1) if vs else 'hand'
        return {'derived':r'S$_{\mathrm{paper}}$ (derived, Rust)','zkderived':r'S$_{\mathrm{zab}}$ (derived, Rust)','swiftderived':r'S$_{\mathrm{swift}}$ (derived, Rust)'}.get(w, r'S$_{\mathrm{raft}}$ (hand-written, Rust)')
    return r'S$_{\mathrm{'+esc(P)+'}}$'
_st_src=open(__import__('os').path.join(__import__('os').path.dirname(__import__('os').path.abspath(__file__)),'study-tables.py')).read()
_ST={}; exec(compile(_st_src[:_st_src.index('\nCOMMON = ')],'study-tables.py','exec'),_ST)   # the case studies' role names (2026-11-07, audit 1107-0005: CROSS/TWIN mislabelled craq and chainpaxos rows); omnipaxos keeps its own names below
STUDY_TITLE={'pineapple':'Pineapple','swiftpaxos':'SwiftPaxos','chainpaxos':'ChainPaxos','cassandra':'Cassandra LWT','cassandra-rr':'Cassandra RR','zookeeper':'ZooKeeper','craq':'3FS CRAQ','bk':'BookKeeper','crdb':'CockroachDB','scalardb':'ScalarDB'}
def study_pair(r):
    for k,S in _ST['STUDIES'].items():
        if k=='omnipaxos': continue
        try:
            if not S['sel'](r): continue
            ro=S['roles'](r['run id'])
        except Exception: continue
        if ro: t=STUDY_TITLE.get(k,k); return (t+' '+ro[0], ro[1])
    return None
def pair(r,f):   # the two components of a check, as the paper's Table 7 names them: implementation $\sqsubseteq$ specification, each with its origin and language
    P=r['program']; v=r['variant'] or ''; sv=r.get('spec') or v
    _vs=re.search(r'\.vs-([a-z0-9-]+)\.', r.get('run id','')+'.')   # 2026-11-07 (audit 1107-0240): a twin row checked against another variant's export (.vs-correct) names that variant as its specification
    if _vs and _vs.group(1) not in ('hand','derived','zkderived','swiftderived'): sv=_vs.group(1)
    sp=study_pair(r)
    if sp: return sp
    vv=lambda x: '' if x in ('correct','impl','',P) else ', '+esc(x)
    hand=lambda x: esc(P)+' (hand-written, Rust'+vv(x)+')'; twin=lambda x: esc(P)+' Java twin (context-aware agent'+vv(x)+')'; port=lambda x: esc(P)+' Java port (context-free agent'+vv(x)+')'
    SR=r'S$_{\mathrm{'+esc(P)+'}}$ (hand-written, Rust)'; SJ=r'S$_{\mathrm{'+esc(P)+'}}$ Java twin (context-aware agent)'
    if P.startswith('omnipaxos') and 'twin-' in f:   # 2026-11-07 (audit 1107-1727): the OmniPaxos rows of the context-free twin files are Java ports against each other (jimpl-jimpl, the vs- word names the reference port) or against the Rust side they port
        RS={'omnipaxos-paper':'OmniPaxos (library, Rust)','omnipaxos-paper-sol':'OmniPaxos (library, Rust)','omnipaxos-rust':'OmniPaxos (library, Rust)','omnipaxos-rust-sol':'OmniPaxos (library, Rust)','omnipaxos-spec':r'S$_{\mathrm{raft}}$ (hand-written, Rust)','omnipaxos-spec-sol':r'S$_{\mathrm{raft}}$ (hand-written, Rust)','omnipaxos-derived-impl':r'S$_{\mathrm{paper}}$ (derived, Rust)','omnipaxos-derived-sol':r'S$_{\mathrm{paper}}$ (derived, Rust)'}
        if 'jimpl-jimpl' in r.get('run id','') and _vs: return (ONAME.get(P,P), ONAME.get('omnipaxos-'+_vs.group(1), 'omnipaxos-'+_vs.group(1)+' port (Java)'))
        if 'twin-offline-java-spec' in f or 'twin-online-rust-impl' in f: return (RS.get(P,esc(P)+' (Rust)'), ONAME.get(P,P))
        return (ONAME.get(P,P), RS.get(P,esc(P)+' (Rust)'))
    if 'twin-offline-java-spec' in f or 'twin-online-rust-impl' in f: return (port(v) if 'cftwin' in f else hand(v), port(sv)) if 'cftwin' in f and False else ((hand(v), port(sv)) if 'cftwin' in f else (hand(v), twin(sv)))
    if 'twin-offline-rust-spec' in f or 'twin-online-java-impl' in f: return (port(v), hand(sv)) if 'cftwin' in f else (twin(v), hand(sv))
    realsw = P in REAL
    if f in ('campaign-offline-rust-spec.csv','campaign-online-java-impl.csv'):   # a Java implementation against the Rust specification's export
        if P=='raft': return ('MicroRaft (library, Java)', spec_of(P,r))
        if P.startswith('omnipaxos'): return (ONAME.get(P,P), spec_of(P,r))
        if P in AGENTIC: return (esc(P)+' (context-free agent, Java)', SR)
        return (twin(v), SR)
    if f in ('campaign-offline-java-spec.csv','campaign-online-rust-impl.csv'):   # a Rust implementation against the Java specification's export
        if P=='raft': return ('raft-rs (library, Rust)', r'S$_{\mathrm{raft}}$ Java twin (context-aware agent)')
        if P in AGENTIC: return (esc(P)+' (context-free agent, Rust)', SJ)
        return (hand(v), SJ)
    if f in ('campaign-offline-tf.csv','campaign-stateful.csv'):   # Rust against Rust
        if P=='raft': return ('raft-rs (library, Rust)', r'S$_{\mathrm{raft}}$ (hand-written, Rust)')
        if P=='omnipaxos' and '.spec_' in r['run id']: return (r'S$_{\mathrm{raft}}$ (hand-written, Rust)', spec_of(P,r))   # the two specifications against each other
        if P=='omnipaxos': return ('OmniPaxos (library, Rust'+vv(v)+')', spec_of(P,r))   # the variant (reorder) after the comma, as for every program (audit 1107-1409)
        if P=='omnipaxos-derived': return (r'S$_{\mathrm{paper}}$ (derived, Rust)', spec_of(P,r))
        if P in AGENTIC: return (esc(P)+' (context-free agent, Rust)', SR)
        return (hand(v), SR+vv(sv))
    return (esc(short(P,v)), '?')
def ekey(r):   # 2026-11-07 (audit 1107-1727): the reference named by the vs- word and the Java-against-Java marker are part of the instance; the online Rust-against-Java-port row must not merge with a port-against-port offline row
    rid=r.get('run id','')+'.'; vs=re.search(r'\.vs-([a-z0-9-]+)\.', rid)
    return (r.get('spec','') or '')+'|'+(vs.group(1) if vs else '')+('|jj' if 'jimpl-jimpl' in rid else '')
def inst(r,f):
    m={'bag':'B','fifo':'F','p2p':'F','asyn':'B','causal':'C'}.get(r['model'],'')
    a,b=pair(r,f); sz=r['size']
    if study_pair(r):   # a case-study row: the size with its variant, as the study tables print it (chainpaxos mixed/local)
        v=r.get('variant') or ''; sz+=(' '+esc(v)) if v and v not in ('impl','spec','correct','v1') and v not in r['size'] else ''
    return a+r' $\sqsubseteq$ '+b+(r'$^{\mathrm{'+m+'}}$' if m else '')+'('+sz+')'
NEW_FROM='2026-11-03T23:30'
def paint(r,cells): c='blue' if (r.get('started') or '')>=NEW_FROM else ('orange' if r.get('contended')=='yes' else 'red'); return [r'\textcolor{'+c+'}{'+x+'}' for x in cells]
def status(r):
    v=r['verdict']; return r'\faClock' if v=='t/o' else ('oom' if v=='oom' else ('--' if v=='' else None))
def off_cells(r):  # M = max of the three peaks; T_exp = longer export; T_join; T = T_exp + T_join
    st=status(r)
    M=mib(r['peak MB'])   # tables.py composes it: the two exports' peaks summed (they run side by side), or the join's peak if larger
    try:
        if (r.get('spec export s') or '') == '' or (r.get('impl export s') or '') == '' or (r.get('join s') or '') == '': te=tj=tot=None   # a component without a measurement: no zero
        else: te=max(float(r['spec export s']), float(r['impl export s'])); tj=float(r['join s']); tot=te+tj
    except Exception: te=tj=tot=None
    if te is None and not st: return [M, '--', '--', '--']
    if st: return [M, fnum(te) if te is not None else '--', st, st]
    return [M, fnum(te), fnum(tj), fnum(tot)]
def on_cells(r):  # M of the online run; T_exp = the specification side (in-process: the spec phase of the run; cross-language: the export it loaded); T_run = the implementation phase (in-process) or the online run; T = sum (in-process: the run's wall)
    st=status(r); M=mib(r['peak MB'])
    try:
        if float(r.get('spec phase s') or 0) > 0 and float(r.get('impl phase s') or 0) > 0:   # in-process: the phases are measured; an online run against an export records 0 phases
            te=float(r['spec phase s']); tr=float(r['impl phase s']); tot=float(r['wall s']); oth=fnum(tot-te-tr) if tot-te-tr>=0.005 else '$<$0.01'
        elif (r.get('spec export s') or '') != '':
            te=float(r['spec export s']); tr=float(r.get('run s') or r['wall s']); tot=float(r['wall s']); oth='--'   # tables.py: wall = export + run
        else: te=tr=tot=oth=None   # the components are not recorded: never print a zero for a missing measurement
    except Exception: te=tr=tot=oth=None
    if te is None and not st: return [M, '--', '--', '--', fnum(r['wall s'])]
    if st: return [M, fnum(te) if te is not None else '--', st, '--', st]
    return [M, fnum(te), fnum(tr), oth, fnum(tot)]
NOVERDICT=[]   # (kind, row): runs that ended without a verdict (a failure of the tool, or no verdict and no limit): their own table, never a conforming row
def load(f):
    rows=list(csv.DictReader(open(R+'/PLDI/tex/tables/'+f+'.csv')))
    if MODE=='realsw': rows=[r for r in rows if r['program'] in REAL]
    keep=[]
    for r in rows:
        if r['verdict'] in ('conforms','violation','t/o','oom'): keep.append(r)
        else: NOVERDICT.append(('offline' if 'offline' in f else 'online', f, r))
    return keep
def viol_of(r): return r['verdict'] == 'violation'   # the actual verdict (Opus audit 11-06: classifying by the expected verdict hid real violations)
TITLE={'twin':'Cross-language twin checks: a Java implementation against a Rust implementation of one specification','cross':'Cross-language refinement checks: an implementation in one language against the specification in the other','realsw':'Real software and agentic benchmarks: specification against each implementation'}[MODE]
L=[r'\documentclass[9pt]{article}',r'\usepackage[a4paper,landscape,margin=12mm]{geometry}',r'\usepackage{booktabs,longtable,amsmath,xcolor,fontawesome5}',r'\usepackage[T1]{fontenc}',r'\setlength{\tabcolsep}{3.5pt}',r'\begin{document}',r'\section*{'+TITLE+'}',
   r"Every row is one instance (program, variant, size), the largest size, per communication model, at which both the offline and the online check of the same side exist; one run each; a time or memory limit is shown only from the 48 h tier. Sums of T are over the CSV's values rounded to 0.1 s, so a total can differ from the raw sum by 0.1 s. Instance = the two components of the check as the paper's Table 7 names them, implementation $\sqsubseteq$ specification, each with its origin (hand-written; Java twin by a context-aware agent; Java port by a context-free agent; a library; a context-free agent's program from a paper) and its language, a variant other than the correct one after the comma, then the size in parentheses; the superscript is the communication model (B = bag/asyn, F = fifo/p2p). $G_{\mathit{cand}}$, $G_{\mathit{ref}}$ = the graphs of the checked side and of the export it is checked against; $G_x$ = the checked side's graphs that no graph of the other side covers (violating tables). Online, in-process (Table 1): one run explores the specification's graphs and indexes their summaries, then explores the implementation's graphs and queries the index: M = its peak memory (MiB), T$_{\mathit{exp}}$ = its specification phase, T$_{\mathit{run}}$ = its implementation phase (exploration and queries), T$_{\mathit{oth}}$ = the rest of the run outside the two phase timers (process start, the index of the specification summaries, the report of the uncovered graphs after a violation, shutdown), T = its wall time = T$_{\mathit{exp}}$ + T$_{\mathit{run}}$ + T$_{\mathit{oth}}$; for a cross-language online row T$_{\mathit{oth}}$ is -- and T is the sum of the two runs. Online, cross-language (Tables 2--3): M = the larger of the export's peak and the run's (the export ran earlier, alone), T$_{\mathit{exp}}$ = the export run of the specification side (its exploration, summarising and sorted distinct store), T$_{\mathit{run}}$ = the online run that loads it and explores the implementation, T = their sum. Offline: each side's export run explores its graphs and writes the sorted set of its distinct summaries; the join compares the two sets: M = the sum of the two exports' peaks (the two jobs run side by side and are resident at once), or the join's peak if that is larger, T$_{\mathit{exp}}$ = the longer export run, T$_{\mathit{join}}$ = the join (small, because the exports are already distinct and sorted: hermes(3,3)'s 90\,M implementation graphs export as 470 summaries), T = their sum. Times in seconds, 1 decimal from 10 s, 2 decimals below, 3 below 0.1 s; a run below the 10 ms resolution of the timer prints as $<$0.01; a component without a measurement prints as --, never as 0. \faClock\ = time limit reached, oom = memory limit reached, -- = no verdict. \textcolor{red}{Red} = a run of the earlier campaign that had its NUMA node to itself; \textcolor{orange}{orange} = a run of the earlier campaign that shared its NUMA node with another run for more than 30 s (the overlapping queues before the evening of 2026-11-03), whose times are not to be trusted; \textcolor{blue}{blue} = a run started after 2026-11-03 23:30 under the one-row-per-node rule. Instances with only one of the two checks follow in the per-kind tables.",r'\medskip', open(R+'/plan/twin-tables/BENCHMARKS.tex').read(), r'\medskip']
HDR=lambda viol: (r'\multicolumn{'+str(4 if viol else 3)+r'}{c}{} & \multicolumn{5}{c}{online} & \multicolumn{4}{c}{offline} \\ \cmidrule(lr){'+str(5 if viol else 4)+'-'+str(9 if viol else 8)+r'}\cmidrule(lr){'+str(10 if viol else 9)+'-'+str(13 if viol else 12)+r'}'+'\n'+r'instance & $G_{\mathit{cand}}$ & $G_{\mathit{ref}}$'+(r' & $G_x$' if viol else '')+r' & M & T$_{\mathit{exp}}$ & T$_{\mathit{run}}$ & T$_{\mathit{oth}}$ & T & M & T$_{\mathit{exp}}$ & T$_{\mathit{join}}$ & T \\')
leftover=[]
for off_f,on_f,title in PAIRS:
    off=load(off_f); on=load(on_f)
    if not off and not on: continue
    key=lambda r:(r['program'],r['variant'],ekey(r),r['size'],r['model'])
    offd={key(r):r for r in off}; ond={key(r):r for r in on}
    both=set(offd)&set(ond)
    for viol in (False,True):
        pairs={}
        for k in both:
            r=offd[k]
            if viol_of(r)!=viol: continue
            if r['verdict'] in ('t/o','oom') and (r.get('timeout s') or '') != '172800': continue   # a limit marker only at the 48 h tier (the paper's rule)
            pk=(k[0],k[1],k[2],k[4]); g=tuple(int(t) if t.isdigit() else 0 for t in k[3].split(','))   # the largest size, per model
            if pk not in pairs or g>pairs[pk][0]: pairs[pk]=(g,k)
        rows=[(offd[k],ond[k]) for _,k in pairs.values()]
        if not rows: continue
        def tot(x):
            try: return float(x[0]['wall s'])
            except Exception: return -1
        rows.sort(key=lambda x:(-tot(x), x[0]['program']))
        L+=[r'\begin{longtable}{@{}l'+('rrr' if viol else 'rr')+r'rrrrrrrrr@{}}', r'\caption{'+esc(title)+': '+('violating' if viol else 'conforming')+' pairs ('+str(len(rows))+r' instances).}\\', r'\toprule', HDR(viol), r'\midrule', r'\endfirsthead', r'\toprule', HDR(viol), r'\midrule', r'\endhead', r'\bottomrule', r'\endfoot']
        for ro,rn in rows:
            cells=[inst(ro,off_f+'.csv'), ro['impl graphs'] or '--', ro['spec graphs'] or '--']+([ro.get('uncovered','') or rn.get('uncovered','') or '--'] if viol else [])
            L.append(' & '.join(paint(ro,cells)+paint(rn,on_cells(rn))+paint(ro,off_cells(ro)))+r' \\')
        L+=[r'\end{longtable}']
    # leftovers: instances with only one of the two
    for name,d,kind in ((off_f,offd,'offline'),(on_f,ond,'online')):
        rest=[r for k,r in d.items() if k not in both]
        if rest: leftover.append((title+' ('+kind+' only)',kind,rest,name+'.csv'))
for title,kind,rest,name in leftover:
    for viol in (False,True):
        best={}
        for r in rest:
            if viol_of(r)!=viol: continue
            if r['verdict'] in ('t/o','oom') and (r.get('timeout s') or '') != '172800': continue
            pk=(r['program'],r['variant'],ekey(r),r['model']); g=tuple(int(t) if t.isdigit() else 0 for t in r['size'].split(','))
            if pk not in best or g>best[pk][0]: best[pk]=(g,r)
        rows=[r for _,r in best.values()]
        if not rows: continue
        rows.sort(key=lambda r:(-(float(r['wall s']) if re.fullmatch(r'[0-9.]+',r['wall s'] or '') else -1), r['program']))
        hdr=r'instance & $G_{\mathit{cand}}$ & $G_{\mathit{ref}}$'+(r' & $G_x$' if viol else '')+r' & M & T$_{\mathit{exp}}$ & T$_{\mathit{'+('join' if kind=='offline' else 'run')+r'}}$'+('' if kind=='offline' else r' & T$_{\mathit{oth}}$')+r' & T \\'
        L+=[r'\begin{longtable}{@{}l'+('rrr' if viol else 'rr')+r'rrrr'+('' if kind=='offline' else 'r')+r'@{}}', r'\caption{'+esc(title)+': '+('violating' if viol else 'conforming')+' pairs ('+str(len(rows))+r' instances).}\\', r'\toprule', hdr, r'\midrule', r'\endfirsthead', r'\toprule', hdr, r'\midrule', r'\endhead', r'\bottomrule', r'\endfoot']
        for r in rows:
            cells=[inst(r,name), r['impl graphs'] or '--', r['spec graphs'] or '--']+([r.get('uncovered','') or '--'] if viol else [])+(off_cells(r) if kind=='offline' else on_cells(r))
            L.append(' & '.join(paint(r,cells))+r' \\')
        L+=[r'\end{longtable}']
if NOVERDICT:
    L+=[r'\subsection*{Runs that ended without a verdict}', r'These runs are in no table above: the tool stopped by itself without a verdict (its last line is quoted), or the record carries neither a verdict nor a limit. They stand until rerun at the current pin or explained.', r'\medskip',
        r'\begin{longtable}{@{}llrrp{95mm}@{}}', r'\caption{Runs without a verdict ('+str(len(NOVERDICT))+r').}\\', r'\toprule', r'instance & kind & T$_{\mathit{run}}$ & M & outcome \\', r'\midrule', r'\endfirsthead', r'\toprule', r'instance & kind & T$_{\mathit{run}}$ & M & outcome \\', r'\midrule', r'\endhead', r'\bottomrule', r'\endfoot']
    for kind,f,r in sorted(NOVERDICT, key=lambda x:(x[2]['program'], x[2]['run id'])):
        why=(r.get('stop reason') or 'no stop reason')+((': '+esc(r['failure'])) if r.get('failure') else '')+r' \quad {\footnotesize\texttt{'+esc(r['run id'])+'}}'
        L.append(' & '.join(paint(r,[inst(r,f), kind, fnum(r['wall s']), mib(r['peak MB'])])+[why])+r' \\')
    L+=[r'\end{longtable}']
L.append(r'\end{document}')
OUT={'twin':'TWIN-RESULTS','cross':'CROSS-RESULTS','realsw':'REALSW-RESULTS'}[MODE]
open(R+'/plan/twin-tables/'+OUT+'.tex','w').write('\n'.join(L)+'\n'); print(MODE,'written')
