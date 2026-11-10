import csv,pickle,collections,re,sys,json
G='/local/home/mkhoshechin/Documents/github-jmc/'
idx=pickle.load(open('/tmp/su1110/rows.pkl','rb'))
recs=pickle.load(open('/tmp/su1110/all.pkl','rb'))
R=collections.defaultdict(list)
for x in recs: R[x['run_id']].append(x)
SUF=re.compile(r'^(t\d+|c48h|c1h|cinf|j\d|m|r\d+|p|x\d+|f\d+|w\d+|g|failed-\d+)$')
def base(i):
    p=i.split('.'); rest='.'.join(p[2:]) if len(p)>2 else ''
    toks=sorted(t for t in re.split(r'[._]',rest) if t and not SUF.match(t))
    return p[0]+'.'+(p[1] if len(p)>1 else '')+'.'+'_'.join(toks)
BASES=collections.defaultdict(set)
for i in list(idx)+list(R): BASES[base(i)].add(i)
def rec_of(i):
    rs=R.get(i,[])
    ok=[r for r in rs if r['stop_reason']=='ok']
    return (ok or rs or [None])[-1]
def num(x):
    try: return float(x)
    except: return None
# ---------- export DB
EXRE=re.compile(r'^(C3E|XCE|XJE)\.([^.]+)\.(spec|impl)_([^.]+)\.(.*)$')
def exkey(i):
    m=EXRE.match(i)
    if not m: return None
    pre,d,side,desc,suf=m.groups()
    toks=desc.split('_'); nums=tuple(int(t) for t in toks if t.isdigit()); words=tuple(sorted(t for t in toks if not t.isdigit()))
    tt=[t for t in suf.split('.') if re.match(r'^[tw]\d+$',t)]
    return (pre,d,side,words,tt[0] if tt else ''),nums
EXDB=collections.defaultdict(lambda: collections.defaultdict(list))
for i,rs in R.items():
    k=exkey(i)
    if not k: continue
    for r in rs:
        EXDB[k[0]][k[1]].append((num(r['wall_ms']) and num(r['wall_ms'])/1000, (num(r['peak_rss_kb']) or 0)/1024, r['stop_reason'], i, r['node']))
def ex_ok(k,nums):
    v=[x for x in EXDB[k].get(nums,[]) if x[2]=='ok' and x[0] is not None]
    return max(v,key=lambda x:x[0]) if v else None
def ex_fail(k,nums):
    return [x for x in EXDB[k].get(nums,[]) if x[2]!='ok']
pickle.dump(None,open('/tmp/su1110/dummy','wb'))
T=G+'PLDI/tex/tables/'
def sizekey(s): return [(0,int(t),'') if t.isdigit() else (1,0,t) for t in re.split(r'[, ]',s)]
FAM=collections.defaultdict(lambda: collections.defaultdict(dict))
for fn,kind,dirn in [('campaign-twin-offline-java-spec','twin','J>R'),('campaign-twin-offline-rust-spec','twin','R>J'),('campaign-cftwin-offline-java-spec','cftwin','J>R'),('campaign-cftwin-offline-rust-spec','cftwin','R>J')]:
    for x in csv.DictReader(open(T+fn+'.csv')):
        rid=x['run id']; m=re.search(r'\.vs-([^.]+)\.',rid)
        spec=m.group(1) if m else '='
        x['_csv']=fn
        sw='+'.join(t for t in re.split(r'[, ]',x['size']) if not t.isdigit())   # scenario words of the size (tikv X2, scalardb lost/snapshot, chainpaxos mixed)
        k=(kind,x['directory'],x['program'],x['variant'],spec,x['model']+('/'+sw if sw else ''))
        x['_exports']=re.findall(r'"\$EVAL_OUT"/(?:imports/)?([^/ ]+)/export\.jsonl',idx[rid][0][3])
        if 'jimpl-jimpl' in rid or 'tfimpl-tfimpl' in rid: continue   # same-language pairs are not cross-language twins
        FAM[k][x['size']][dirn]=x
def numpos(s): 
    t=re.split(r'[, ]',s); return [j for j,x in enumerate(t) if x.isdigit()],t
def sub_desc(i,newnums):
    p=i.split('.'); desc=p[2]; toks=desc.split('_'); it=iter(newnums); n=sum(t.isdigit() for t in toks)
    if n!=len(newnums): return None
    toks=[str(next(it)) if t.isdigit() else t for t in toks]
    return p[0],p[1],'_'.join(toks),p[3:]
def sub_cmd(cmd,newnums):
    cmd=re.sub(r'^needok:\S+ ','',cmd); cmd=re.sub(r'^env JAVA_TOOL_OPTIONS=\S+ ','',cmd)
    if 'rust-run.sh' in cmd:
        head,args=cmd.split(' -- ',1); toks=args.split()
        if sum(t.isdigit() for t in toks)!=len(newnums): return None
        it=iter(newnums); args=' '.join(str(next(it)) if t.isdigit() else t for t in toks)
        m=re.search(r'--size (\S+)',head)
        if m:
            st=m.group(1).split(','); it2=iter(newnums)
            if sum(t.isdigit() for t in st)!=len(newnums): return None
            head=head.replace(m.group(0),'--size '+','.join(str(next(it2)) if t.isdigit() else t for t in st))
        return head+' -- '+args
    if 'jmc-run.sh' in cmd:
        m=re.search(r"'([^':]+):([^']*)'",cmd); st=m.group(2).split(',')
        if sum(t.isdigit() for t in st)!=len(newnums): return None
        it=iter(newnums); new=m.group(1)+':'+','.join(str(next(it)) if t.isdigit() else t for t in st)
        cmd=cmd.replace(m.group(0),"'"+new+"'")
        # timeout argument (7th positional) -> 8640000
        cmd=re.sub(r"(export\.jsonl|-) (\d+)( |$)",lambda mm: mm.group(1)+' 8640000'+mm.group(3),cmd,count=1)
        return cmd
    return None
def new_export_id(tid,newnums):
    r=sub_desc(tid,newnums)
    if not r: return None
    pre,d,desc,rest=r; tt=[t for t in rest if re.match(r'^[tw]\d+$',t)]
    return f"{pre}.{d}.{desc}."+('.'.join(tt)+'.' if tt else '')+'cinf'
def new_join_id(tid,newnums):
    r=sub_desc(tid,newnums)
    if not r: return None
    pre,d,desc,rest=r; keep=[t for t in rest if t.startswith('vs-') or re.match(r'^[a-z]+-[a-z]+$',t) and ('impl' in t or 'spec' in t)]
    return f"{pre}.{d}.{desc}."+(''.join(t+'.' for t in keep))+'cinf.m'
def status_of(newid):
    b=base(newid); ex=BASES.get(b,set())
    if not ex: return 'new',[]
    st=[]
    for i in ex:
        r=rec_of(i)
        if r is None: st.append(('queued',i))
        elif r['stop_reason']=='ok': st.append(('done',i))
        else: st.append(('failed:'+r['stop_reason'],i))
    return ('done' if any(s[0]=='done' for s in st) else 'queued' if any(s[0]=='queued' for s in st) else st[0][0]),st
def base2(i):
    p=i.split('.'); rest=p[2:] if len(p)>2 else []
    desc=rest[0] if rest else ''
    toks=desc.split('_'); nums=[t for t in toks if t.isdigit()]; words=sorted(t for t in toks if not t.isdigit())
    tail=sorted(t for t in rest[1:] if not SUF.match(t))
    return (p[0],p[1] if len(p)>1 else '',tuple(words),tuple(nums),tuple(tail))
BASES2=collections.defaultdict(set)
for i in list(idx)+list(R): BASES2[base2(i)].add(i)
def status_of(newid):
    ex=BASES2.get(base2(newid),set())
    if not ex: return 'new',[]
    st=[]
    for i in ex:
        r=rec_of(i)
        if r is None: st.append(('queued',i))
        elif r['stop_reason']=='ok': st.append(('done',i))
        else: st.append(('failed:'+r['stop_reason'],i))
    return ('done' if any(s[0]=='done' for s in st) else 'queued' if any(s[0]=='queued' for s in st) else st[0][0]),st
AX={}
for l in open(G+'daavar/cluster/campaign/inputs/grid-axes.tsv'):
    if l.startswith('#') or not l.strip(): continue
    f=l.rstrip('\n').split('\t'); AX[f[0]]=([x=='1' for x in f[1].split()], [int(x) if x.isdigit() else None for x in f[2].split()] if len(f)>2 and f[2].strip() else None)
def fnum(x):
    try: return float(x)
    except: return None
def nums_of(s):
    pos,t=numpos(s); return tuple(int(t[j]) for j in pos)
def ex_time(eid):
    k=exkey(eid)
    if not k: return None
    return ex_ok(k[0],k[1])
def axis_ratio(eid, axis, P):
    """time and memory ratio of export eid's key along axis: the closest measured step Q->Q+e_axis (both ok), Q<=P"""
    k=exkey(eid); key=k[0]; best=None
    for q,v in EXDB[key].items():
        q2=list(q)
        if len(q2)<=axis: continue
        q2[axis]+=1; q2=tuple(q2)
        a=ex_ok(key,q); b=ex_ok(key,q2)
        if a and b and a[0] is not None and b[0] is not None:
            if best is None or q2>best[0]: best=(q2,a,b)
    if not best: return None
    q2,a,b=best
    tr=b[0]/max(a[0],0.5); mr=max(b[1],1)/max(a[1],1)
    return dict(step=f"{a[3]} {a[0]:.1f}s -> {b[3]} {b[0]:.1f}s",tr=max(tr,1.0),mr=max(mr,1.0),small=a[0]<2)
def iters(e):
    if not e: return None
    r=rec_of(e[3]); 
    try: return float(r['iterations'] or 0) or None
    except: return None
def graph_ratio(key,axis):
    best=None
    for q in EXDB[key]:
        q2=list(q)
        if len(q2)<=axis: continue
        q2[axis]+=1; q2=tuple(q2)
        a=ex_ok(key,q); b=ex_ok(key,q2)
        ia,ib=iters(a),iters(b)
        if ia and ib:
            if best is None or q2>best[0]: best=(q2,ib/ia,a,b)
    return best
def rate_ref(key):
    # per-graph time of the same export (same tool, directory, side, words), the smallest over its ok runs of >= 30 s
    c=[]
    for q in EXDB[key]:
        e=ex_ok(key,q); it=iters(e)
        if e and it and it>=1000 and e[0]>=30: c.append((e[0]/it,e[0],e[3]))
    if not c: return None
    r=min(c); return (r[1],r[0],r[2])
def est_export(tid, P, axis, other=None):
    k=exkey(tid); key=k[0]; e=ex_ok(key,P)
    if not e: return dict(err='no ok record of template at P')
    gr=graph_ratio(key,axis); src='same export'
    if not gr and other:
        gr=graph_ratio(exkey(other)[0],axis); src='other-language twin export'
    if not gr:
        # any axis
        cands=[graph_ratio(key,a) for a in range(len(P))]; cands=[c for c in cands if c]
        if cands: gr=max(cands,key=lambda c:c[1]); src='other axis (max)'
    if not gr: return dict(err='no growth measure',T=e[0],M=e[1],tid=e[3],node=e[4])
    g=max(gr[1],1.0); it=iters(e) or 0
    if e[0]>=30 or not it: Test=e[0]*g; how=f"T(P)*g"
    else:
        rr=rate_ref(key)
        if rr: Test=e[0]+rr[1]*it*(g-1); how=f"T(P)+rate({rr[2]})*G(P)*(g-1)"
        else: Test=e[0]*g; how="T(P)*g"
    a,b=gr[2],gr[3]; mr=max(b[1],1)/max(a[1],1)
    Mest=e[1]*max(1.0,min(g,mr if a[1]>200 else g))
    return dict(T=e[0],M=e[1],G=it,tid=e[3],node=e[4],g=g,gstep=f"{a[3]}({iters(a):.0f}) -> {b[3]}({iters(b):.0f}) [{src}]",Test=Test,Mest=Mest,Gest=it*g,how=how)
def steps(key,axis):
    out=[]
    for q in EXDB[key]:
        q2=list(q)
        if len(q2)<=axis: continue
        q2[axis]+=1; q2=tuple(q2)
        a=ex_ok(key,q); b=ex_ok(key,q2); ia,ib=iters(a),iters(b)
        if ia and ib: out.append((q,q2,ib/ia,a,b))
    return out
def pick_step(key,axis,P):
    st=steps(key,axis)
    if not st: return None,None
    def others(q): return tuple(x for j,x in enumerate(q) if j!=axis)
    ex=[s for s in st if s[1]==P]
    same=[s for s in st if others(s[1])==others(P) and s[1][axis]<=P[axis]]
    pool=ex or sorted(same,key=lambda s:s[1]) or sorted(st,key=lambda s:s[1])
    last=pool[-1] if not ex else ex[0]
    prev=[s for s in st if s[1]==last[0]]
    return last,(prev[0] if prev else None)
def est_export(tid, P, axis, other=None):
    k=exkey(tid); key=k[0]; e=ex_ok(key,P)
    if not e: return dict(err='no ok record of the template export at the base size')
    src='same export'; last,prev=pick_step(key,axis,P)
    if not last and other:
        ok_=exkey(other)[0]; last,prev=pick_step(ok_,axis,P); src='twin export in the other language'
    if not last:
        c=[pick_step(key,a,P) for a in range(len(P)) if a!=axis]; c=[x for x in c if x[0]]
        if c: last,prev=max(c,key=lambda x:x[0][2]); src='another axis (largest step)'
    if not last: return dict(err='no growth measure',T=e[0],M=e[1],tid=e[3],node=e[4])
    g=max(last[2],1.0); acc=''
    if prev and prev[2]>=1 and last[2]>prev[2]:
        g=g*(last[2]/prev[2]); acc=f" x accel {last[2]:.1f}/{prev[2]:.1f}"
    it=iters(e) or 0
    if e[0]>=30 or not it: Test=e[0]*g; how="T(P)*g"
    else:
        rr=rate_ref(key)
        if rr and rr[1]*it*g>e[0]: Test=e[0]+rr[1]*it*(g-1); how=f"T(P)+r*G(P)*(g-1), r from {rr[2]}"
        else: Test=e[0]*g; how="T(P)*g"
    a,b=last[3],last[4]; mr=max(b[1],1)/max(a[1],1)
    if key[0]=='C3E': Mest=e[1]*max(1.0,min(mr,g))
    else: Mest=e[1]*(max(1.0,min(mr,g)) if a[1]>500 else g**0.6)
    return dict(T=e[0],M=e[1],G=it,tid=e[3],node=e[4],g=g,gstep=f"{a[3]} G={iters(a):.0f} -> {b[3]} G={iters(b):.0f} ({last[2]:.1f}x{acc}; {src})",Test=Test,Mest=Mest,Gest=it*g,how=how)
def family_eval(k, v):
    kind,d,prog,var,spec,model=k
    ss=sorted(v,key=sizekey)
    S={}
    for s in ss:
        try: S[nums_of(s)]=s
        except: pass
    allc=all(x.get('verdict')=='conforms' for s in ss for x in v[s].values())
    if not S: return dict(k=k,allc=allc,sizes=ss,cands=[],why='no numeric size')
    dim=len(next(iter(S)))
    axes,mx=AX.get(d.replace('cf-',''),([True]*dim,None))
    axes=(axes+[True]*dim)[:dim]
    front=[p for p in S if not any(q!=p and all(q[j]>=p[j] for j in range(dim)) for q in S)]
    cands=[]
    for P in front:
        s=S[P]; rows=v[s]
        for ax in range(dim):
            if not axes[ax]: continue
            C=list(P); C[ax]+=1; C=tuple(C)
            if mx and mx[ax] is not None and C[ax]>mx[ax]: cands.append(dict(P=P,C=C,ax=ax,skip=f'program bound {mx[ax]} on axis {ax+1}')); continue
            if C in S: continue
            ex={}; pairs={}
            for dirn,x in rows.items():
                for e in x['_exports']: ex[e]=None
            jr=rows.get('J>R',{}).get('_exports',[]); rj=rows.get('R>J',{}).get('_exports',[])
            if len(jr)==2 and len(rj)==2: pairs={jr[0]:rj[0],rj[0]:jr[0],jr[1]:rj[1],rj[1]:jr[1]}
            exs=[]; bad=None
            for e in ex:
                nid=new_export_id(e,C)
                cmd=sub_cmd(idx[e][0][3],C) if e in idx else None
                if not nid or not cmd: bad=f'template mismatch {e}'; break
                st=status_of(nid); est=est_export(e,P,ax,pairs.get(e))
                exs.append(dict(tid=e,id=nid,cmd=cmd,st=st,est=est))
            if bad: cands.append(dict(P=P,C=C,ax=ax,skip=bad)); continue
            js=[]
            for dirn,x in rows.items():
                nid=new_join_id(x['run id'],C)
                ne=[new_export_id(e,C) for e in x['_exports']]
                jt=fnum(x.get('join s')) or 0; jm=fnum(x.get('join peak MB')) or 0
                js.append(dict(dirn=dirn,tid=x['run id'],id=nid,exports=ne,st=status_of(nid),T=jt,M=jm,verdict=x['verdict']))
            cands.append(dict(P=P,C=C,ax=ax,exports=exs,joins=js))
    return dict(k=k,allc=allc,sizes=ss,S=S,cands=cands,front=front)
def cand_summary(f,c):
    if 'skip' in c: return None
    st=[e['st'][0] for e in c['exports']]+[j['st'][0] for j in c['joins']]
    blocked=[x for x in c['exports']+c['joins'] if x['st'][0] not in ('new','done')]
    jdone=[j for j in c['joins'] if j['st'][0]!='new']
    errs=[e for e in c['exports'] if 'err' in e['est']]
    T=sum(e['est'].get('Test',0) for e in c['exports'] if 'Test' in e['est'])
    gm=max([e['est'].get('g',1) for e in c['exports'] if 'g' in e['est']] or [1])
    TJ=sum(max(j['T'],0.1)*gm for j in c['joins'])
    M=max([e['est'].get('Mest',0) for e in c['exports']]+[j['M']*gm for j in c['joins']] or [0])
    return dict(T=T+TJ,Texp=T,TJ=TJ,M=M,blocked=blocked,jdone=jdone,errs=errs)
STALE=('phase1/','phase2/','phase3/','scaling/','scaling-cf/','frontier/','cf2/','twins/','twins-cf/','twins-cf3/','scaling-next/')
def status_of(newid):
    ex=BASES2.get(base2(newid),set())
    if not ex: return 'new',[]
    st=[]
    for i in ex:
        r=rec_of(i)
        if r is None:
            fs=[x[0] for x in idx.get(i,[])]
            st.append(('stale' if fs and all(f.startswith(STALE) for f in fs) else 'queued',i))
        elif r['stop_reason']=='ok': st.append(('done',i))
        else: st.append(('failed:'+r['stop_reason'],i))
    ss=[s[0] for s in st]
    for k in ('queued','done'):   # a pending row of the same instance anywhere blocks, even when another record is done
        if k in ss: return k,st
    fl=[s for s in ss if s.startswith('failed')]
    if fl: return fl[0],st
    return 'new',st   # only stale rows-file entries (never dispatched)
import glob
SIDE={}
for fn in glob.glob(G+'daavar/cluster/campaign/**/*.rows.tsv',recursive=True):
    try:
        for row in csv.DictReader(open(fn,errors='replace'),delimiter='\t'):
            if row.get('id'): SIDE.setdefault(row['id'],row)
    except Exception: pass
HDR="id\ttable\tfamily\tsize\tmodel\tvariant\tengine\tthreads\texpected_verdict\tnote\ttier\tkind\tspec_tool\timpl_tool\tspec_lang\timpl_lang\tmode\texploration\tbenchmark\tspec_case\timpl_case".split('\t')
def side_of(tid):
    if tid in SIDE: return dict(SIDE[tid])
    t=tid
    for _ in range(4):
        t2=re.sub(r'\.(c48h|c1h|cinf|m|r\d+|j\d|p|g|x\d+)$','',t)
        if t2==t: break
        t=t2
        if t in SIDE: return dict(SIDE[t])
    # any sidecar line whose base2 matches
    return None
