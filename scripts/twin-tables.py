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
def esc(s): return s.replace('_','\\_').replace('%','\\%')
def fnum(x):
    try: v=float(x); return str(int(v)) if v==int(v) else '%.1f' % v
    except Exception: return '--'
def mib(x):   # the CSV's peak MB is already KiB/1024, i.e. MiB (audit 2026-11-04: no second division)
    try: return str(round(float(x)))
    except Exception: return '--'
def gi(x):
    try: return int(float(x))
    except Exception: return -1
def inst(r):
    m={'bag':'B','fifo':'F','p2p':'F','asyn':'B','causal':'C'}.get(r['model'],'')
    return r'\textsf{'+esc(short(r['program'],r['variant']))+'}'+(r'$^{\mathrm{'+m+'}}$' if m else '')+'('+r['size']+')'
NEW_FROM='2026-11-03T23:30'
def paint(r,cells): c='blue' if (r.get('started') or '')>=NEW_FROM else ('orange' if r.get('contended')=='yes' else 'red'); return [r'\textcolor{'+c+'}{'+x+'}' for x in cells]
def status(r):
    v=r['verdict']; return r'\faClock' if v=='t/o' else ('oom' if v=='oom' else ('--' if v=='' else None))
def off_cells(r):  # M = max of the three peaks; T_exp = longer export; T_join; T = T_exp + T_join
    st=status(r)
    peaks=[gi(r.get(k) or -1) for k in ('spec export peak MB','impl export peak MB','join peak MB')]; M=mib(max(peaks)) if max(peaks)>=0 else mib(r['peak MB'])
    try: te=max(float(r.get('spec export s') or 0), float(r.get('impl export s') or 0)); tj=float(r.get('join s') or 0); tot=te+tj
    except Exception: te=tj=tot=None
    if st: return [M, fnum(te) if te is not None else '--', st, st]
    return [M, fnum(te), fnum(tj), fnum(tot)]
def on_cells(r):  # M of the online run; T_exp of the specification side; T_run; T = sum
    st=status(r); M=mib(r['peak MB'])
    try: te=float(r.get('spec export s') or 0); tr=float(r['wall s']); tot=te+tr
    except Exception: te=tr=tot=None
    if st: return [M, fnum(te) if te is not None else '--', st, st]
    return [M, fnum(te), fnum(tr), fnum(tot)]
def load(f):
    rows=list(csv.DictReader(open(R+'/PLDI/tex/tables/'+f+'.csv')))
    if MODE=='realsw': rows=[r for r in rows if r['program'] in REAL]
    return rows
def viol_of(r): return (r['expected']=='violation') if r['expected'] in ('conforms','violation') else (r['verdict']=='violation')
TITLE={'twin':'Cross-language twin checks: a Java implementation against a Rust implementation of one specification','cross':'Cross-language refinement checks: an implementation in one language against the specification in the other','realsw':'Real software and agentic benchmarks: specification against each implementation'}[MODE]
L=[r'\documentclass[9pt]{article}',r'\usepackage[a4paper,landscape,margin=12mm]{geometry}',r'\usepackage{booktabs,longtable,amsmath,xcolor,fontawesome5}',r'\usepackage[T1]{fontenc}',r'\setlength{\tabcolsep}{3.5pt}',r'\begin{document}',r'\section*{'+TITLE+'}',
   r"Every row is one instance (program, variant, size), the largest size, per communication model, at which both the offline and the online check of the same side exist; one run each; a time or memory limit is shown only from the 48 h tier. Sums of T are over the CSV's values rounded to 0.1 s, so a total can differ from the raw sum by 0.1 s. Names as in the paper, the size in parentheses; the superscript is the communication model (B = bag/asyn, F = fifo/p2p). $G_{\mathit{impl}}$, $G_{\mathit{spec}}$ = the graphs of the checked side and of the export it is checked against; $G_x$ = the checked side's graphs that no graph of the other side covers (violating tables). Online: M = peak memory of the online run (MiB), T$_{\mathit{exp}}$ = the export of the specification side, T$_{\mathit{run}}$ = the online run, T = their sum. Offline: M = the largest peak of the two exports and the join, T$_{\mathit{exp}}$ = the longer export, T$_{\mathit{join}}$ = the join, T = their sum. \faClock\ = time limit reached, oom = memory limit reached, -- = no verdict. \textcolor{red}{Red} = a run of the earlier campaign that had its NUMA node to itself; \textcolor{orange}{orange} = a run of the earlier campaign that shared its NUMA node with another run for more than 30 s (the overlapping queues before the evening of 2026-11-03), whose times are not to be trusted; \textcolor{blue}{blue} = a run started after 2026-11-03 23:30 under the one-row-per-node rule. Instances with only one of the two checks follow in the per-kind tables.",r'\medskip', open(R+'/plan/twin-tables/BENCHMARKS.tex').read(), r'\medskip']
HDR=lambda viol: (r'\multicolumn{'+str(4 if viol else 3)+r'}{c}{} & \multicolumn{4}{c}{online} & \multicolumn{4}{c}{offline} \\ \cmidrule(lr){'+str(5 if viol else 4)+'-'+str(8 if viol else 7)+r'}\cmidrule(lr){'+str(9 if viol else 8)+'-'+str(12 if viol else 11)+r'}'+'\n'+r'instance & $G_{\mathit{impl}}$ & $G_{\mathit{spec}}$'+(r' & $G_x$' if viol else '')+r' & M & T$_{\mathit{exp}}$ & T$_{\mathit{run}}$ & T & M & T$_{\mathit{exp}}$ & T$_{\mathit{join}}$ & T \\')
leftover=[]
for off_f,on_f,title in PAIRS:
    off=load(off_f); on=load(on_f)
    if not off and not on: continue
    key=lambda r:(r['program'],r['variant'],r.get('spec',''),r['size'],r['model'])
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
        L+=[r'\begin{longtable}{@{}l'+('rrr' if viol else 'rr')+r'rrrrrrrr@{}}', r'\caption{'+esc(title)+': '+('violating' if viol else 'conforming')+' pairs ('+str(len(rows))+r' instances).}\\', r'\toprule', HDR(viol), r'\midrule', r'\endfirsthead', r'\toprule', HDR(viol), r'\midrule', r'\endhead', r'\bottomrule', r'\endfoot']
        for ro,rn in rows:
            cells=[inst(ro), ro['impl graphs'] or '--', ro['spec graphs'] or '--']+([ro.get('uncovered','') or rn.get('uncovered','') or '--'] if viol else [])
            L.append(' & '.join(paint(ro,cells)+paint(rn,on_cells(rn))+paint(ro,off_cells(ro)))+r' \\')
        L+=[r'\end{longtable}']
    # leftovers: instances with only one of the two
    for name,d,kind in ((off_f,offd,'offline'),(on_f,ond,'online')):
        rest=[r for k,r in d.items() if k not in both]
        if rest: leftover.append((title+' ('+kind+' only)',kind,rest))
for title,kind,rest in leftover:
    for viol in (False,True):
        best={}
        for r in rest:
            if viol_of(r)!=viol: continue
            if r['verdict'] in ('t/o','oom') and (r.get('timeout s') or '') != '172800': continue
            pk=(r['program'],r['variant'],r.get('spec',''),r['model']); g=tuple(int(t) if t.isdigit() else 0 for t in r['size'].split(','))
            if pk not in best or g>best[pk][0]: best[pk]=(g,r)
        rows=[r for _,r in best.values()]
        if not rows: continue
        rows.sort(key=lambda r:(-(float(r['wall s']) if re.fullmatch(r'[0-9.]+',r['wall s'] or '') else -1), r['program']))
        hdr=r'instance & $G_{\mathit{impl}}$ & $G_{\mathit{spec}}$'+(r' & $G_x$' if viol else '')+r' & M & T$_{\mathit{exp}}$ & T$_{\mathit{'+('join' if kind=='offline' else 'run')+r'}}$ & T \\'
        L+=[r'\begin{longtable}{@{}l'+('rrr' if viol else 'rr')+r'rrrr@{}}', r'\caption{'+esc(title)+': '+('violating' if viol else 'conforming')+' pairs ('+str(len(rows))+r' instances).}\\', r'\toprule', hdr, r'\midrule', r'\endfirsthead', r'\toprule', hdr, r'\midrule', r'\endhead', r'\bottomrule', r'\endfoot']
        for r in rows:
            cells=[inst(r), r['impl graphs'] or '--', r['spec graphs'] or '--']+([r.get('uncovered','') or '--'] if viol else [])+(off_cells(r) if kind=='offline' else on_cells(r))
            L.append(' & '.join(paint(r,cells))+r' \\')
        L+=[r'\end{longtable}']
L.append(r'\end{document}')
OUT={'twin':'TWIN-RESULTS','cross':'CROSS-RESULTS','realsw':'REALSW-RESULTS'}[MODE]
open(R+'/plan/twin-tables/'+OUT+'.tex','w').write('\n'.join(L)+'\n'); print(MODE,'written')
