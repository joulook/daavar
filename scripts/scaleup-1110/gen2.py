exec(open('/tmp/su1110/an.py').read())
import glob,os
SV='/home/mkhoshechin/.claude/jobs/28580aa5/tmp/su-survey/'
LIVEF=set(); LIVEIDS=set(); SDONE=set()
for fn in glob.glob(SV+'*.txt'):
    for l in open(fn,errors='replace'):
        p=l.split()
        if not p: continue
        if p[0]=='LIVEQ' and len(p)>4: LIVEF.add(re.sub(r'.*/campaign/','',p[4]))
        elif p[0] in ('PEND','RUN') and len(p)>3: LIVEIDS.add(p[3])
        elif p[0]=='DONE' and len(p)>2: SDONE.add(p[2])
SC=G+'daavar/cluster/campaign/scaleup-1110/'
SCIDS=set()
for fn in glob.glob(SC+'*.rows'):
    if fn.endswith('brain02.rows'): continue
    for l in open(fn):
        if l.strip() and not l.startswith('#'): SCIDS.add(l.split()[0])
LIVEIDS|=SCIDS
for i in LIVEIDS|SDONE: BASES2[base2(i)].add(i)
ALLIDS=set(idx)|set(R)|LIVEIDS|SDONE
def status_of(newid, old=False):
    ex=BASES2.get(base2(newid),set())
    st=[]
    for i in ex:
        r=rec_of(i)
        if r is not None: st.append(('done' if r['stop_reason']=='ok' else 'failed:'+r['stop_reason'],i)); continue
        if i in LIVEIDS: st.append(('queued',i)); continue
        if i in SDONE: st.append(('done-unflushed',i)); continue
        fs=[x[0] for x in idx.get(i,[])]
        if old:
            st.append(('stale' if fs and all(f.startswith(STALE) for f in fs) else 'queued',i))
        else:
            st.append(('queued' if any(f in LIVEF for f in fs) else 'dead',i))
    ss=[s[0] for s in st]
    if 'queued' in ss: return 'queued',st
    if 'done-unflushed' in ss: return 'queued',st   # finished on a node but not in brain-data: its export is not reusable here, and a rerun would duplicate it
    if 'done' in ss: return 'done',st
    fl=[s for s in ss if s.startswith('failed')]
    if fl: return fl[0],st
    return 'new',st
def fresh(i):
    if i not in ALLIDS: return i
    for k in range(2,20):
        if f"{i}.r{k}" not in ALLIDS: return f"{i}.r{k}"
def measured_fix(exs):
    for e in exs:
        if e['st'][0]=='done':
            best=None
            for s_,i in e['st'][1]:
                if s_=='done':
                    r=rec_of(i); w=fnum(r['wall_ms'])
                    if w is not None and (best is None or w>best[0]): best=(w/1000,(fnum(r['peak_rss_kb']) or 0)/1024,i,r['node'])
            if best: e['est']=dict(e['est'],T=best[0],Test=best[0],Mest=best[1],node=best[3],how='measured at this size (re-export)',gstep=f'{best[2]} {best[0]:.1f} s on {best[3]}')
def dominated(exs,C):
    for e in exs:
        k=exkey(e['tid'])
        if not k: continue
        for q,lst in EXDB[k[0]].items():
            if len(q)==len(C) and all(q[j]<=C[j] for j in range(len(q))):
                for x in lst:
                    if x[2] in ('timeout','memory') or x[2].startswith('failed: incomplete'): return f"{x[3]}: {x[2]}"
    return None
out=[family_eval(k,v) for k,v in sorted(FAM.items())]
# previous selection (scale-up 1110): families with a pick, from the 532 rows' sidecars
PREV=set()
for fn in glob.glob(SC+'*.rows.tsv'):
    if 'brain02' in fn: continue
    for r in csv.DictReader(open(fn),delimiter='\t'): PREV.add(r['id'])
def build_step(f,c,k):
    """candidate k steps along c's axis from c['P'] (c is the step-1 candidate)"""
    Ck=list(c['P']); Ck[c['ax']]+=k; Ck=tuple(Ck)
    exs=[]
    for e in c['exports']:
        nid=new_export_id(e['tid'],Ck); cmd=sub_cmd(idx[e['tid']][0][3],Ck)
        est=dict(e['est']); g=est.get('g',1)
        if 'Test' in est and k>1:
            est['Test']=est['Test']*g**(k-1); est['Mest']=est.get('Mest',0)*(max(1.0,est.get('Mest',1)/max(est.get('M',1),1))**(k-1))
            est['how']=est.get('how','')+f'; step {k}: x g^{k-1}'
        exs.append(dict(tid=e['tid'],id=nid,cmd=cmd,st=status_of(nid),ost=status_of(nid,old=True),est=est))
    measured_fix(exs)
    m={e1['id']:e2['id'] for e1,e2 in zip(c['exports'],exs)}
    js=[]
    for j in c['joins']:
        nid=new_join_id(j['tid'],Ck)
        js.append(dict(j,id=nid,exports=[m[x] for x in j['exports']],st=status_of(nid),ost=status_of(nid,old=True)))
    return dict(P=c['P'],C=Ck,ax=c['ax'],k=k,exports=exs,joins=js)
UNITS=[]; LOG=[]
for f in out:
    kind,d,prog,var,spec,model=f['k']
    if d=='tikv-txn': continue
    lab=f"{kind} {prog} [{d}] {var} vs {var if spec=='=' else spec} {model}"
    had_pick=any(i.split('.')[1]==d for i in [])  # placeholder
    for c in f['cands']:
        if 'skip' in c or not c['exports']: continue
        if any('err' in e['est'] for e in c['exports']): continue
        if all(e['est'].get('g',1)<=1.0 for e in c['exports']): continue   # flat axis
        mx=AX.get(d.replace('cf-',''),(None,None))[1]
        for k in range(1,6):
            u=build_step(f,c,k)
            if mx and mx[c['ax']] is not None and u['C'][c['ax']]>mx[c['ax']]: break
            if u['C'] in f.get('S',{}): continue
            allx=u['exports']+u['joins']
            if any(x['id'] in SCIDS for x in allx) and all((x['st'][0] in ('queued',) ) for x in u['joins']):  # this size is in the 532
                continue
            T=sum(e['est'].get('Test',0) for e in u['exports']); gm=max([e['est'].get('g',1) for e in u['exports']]+[1])
            T+=sum(max(j.get('T',0.1),0.1)*gm**k for j in u['joins'])
            M=max([e['est'].get('Mest',0) for e in u['exports']]+[0])
            bad=[x for x in allx if x['st'][0] not in ('new','done')]
            dom=dominated(u['exports'],u['C'])
            if bad or dom or T>30*3600 or M>230*1024:
                LOG.append((lab,u['C'],'blocked' if bad else ('limit '+dom if dom else f'est {T/3600:.0f} h / {M/1024:.0f} GB')))
                break
            prevfam=any(x['id'] in PREV for x in []) 
            oldq=any(x['ost'][0]=='queued' for x in allx)
            u.update(label=lab,allc=f['allc'],fam=f,T=T,M=M,oldq=oldq,phase='C')
            UNITS.append(u)
# category: 1 = a further step of a family already scaled in the 532 (or k>=2), 2 = first step that was > 20 h, 3 = first step formerly blocked by a pending row in a now dead rows file
SCFAM=set()
for i in SCIDS:
    p=i.split('.')
    if len(p)>2: SCFAM.add((p[1],tuple(sorted(t for t in p[2].split('_') if not t.isdigit()))))
for u in UNITS:
    if u['oldq']: u['cat']=3
    elif u['k']==1 and u['T']>20*3600: u['cat']=2
    else: u['cat']=1
print('units',len(UNITS),collections.Counter((u['cat'],u['allc']) for u in UNITS))
# ---- Phase B entries
AXI={'proposals':0,'timeouts':1,'candidates':2}
PBU=[]
def pbu(label,allc,P,C,exports,joins,axis,steps,cat):
    ex=[]
    for t in exports:
        nid=new_export_id(t,C); cmd=sub_cmd(idx[t][0][3],C); est=est_export(t,P,AXI[axis])
        if 'Test' in est and steps>1: est['Test']*=est['g']**(steps-1); est['how']=est.get('how','')+f'; {steps} steps: x g^{steps-1}'
        ex.append(dict(tid=t,id=nid,cmd=cmd,st=status_of(nid),est=est))
    measured_fix(ex)
    js=[]
    for jt,(a,b),dirn,verdict in joins:
        nid=new_join_id(jt,C); js.append(dict(dirn=dirn,tid=jt,id=nid,exports=[ex[a]['id'],ex[b]['id']],st=status_of(nid),T=0.1,M=10))
    T=sum(e['est'].get('Test',0) for e in ex)+1
    PBU.append(dict(label=label,allc=allc,P=P,C=C,k=steps,ax=AXI[axis],exports=ex,joins=js,T=T,M=max(e['est'].get('Mest',0) for e in ex),cat=cat,phase='B',fam=None))
for C,P,ax,st in (((1,4,1),(1,2,1),'timeouts',2),((2,2,3),(2,2,1),'candidates',2),((2,4,1),(2,2,1),'timeouts',2)):
    q='_'.join(map(str,P))
    pbu('S_raft <= S_omni and S_omni <= S_raft (TF/TF)',True,P,C,
        [f'C3E.omnipaxos.spec_correct_{q}_fifo.t16.c48h', f'C3E.omnipaxos-derived.spec_correct_{q}_fifo.t16.c48h' if P!=(1,2,1) else 'C3E.omnipaxos-derived.spec_correct_1_2_1_fifo.t16.c48h.r4'],
        [(f'C3J.omnipaxos.spec_correct_{q}_fifo.vs-derived.c48h.m',(1,0),'S_raft<=S_omni','conforms'),(f'C3J.omnipaxos-derived.spec_correct_{q}_fifo.vs-hand.c48h.m',(0,1),'S_omni<=S_raft','violation')],ax,st,1)
pbu('OmniPaxos <= S_omni (TF/TF)',True,(2,0,1),(3,0,1),['C3E.omnipaxos-derived.spec_correct_2_0_1_fifo.t16.cinf','C3E.omnipaxos.impl_correct_2_0_1_fifo.t16.c48h'],[('C3J.omnipaxos.impl_correct_2_0_1_fifo.vs-derived.cinf.m',(0,1),'R>R','conforms')],'proposals',1,3)
for p in PBU:
    for e in p['exports']:
        if e['id'].startswith('C3E.omnipaxos.spec_correct_') and e['est'].get('T',0)>1000:   # the contended 6436 s record of S_raft (1,2,1): S_raft has 24 graphs
            e['est'].update(Test=0.05,how='S_raft: 24 graphs at (1,e,1); uncontended (1,1,1) time')
    p['T']=sum(e['est'].get('Test',0) for e in p['exports'])+1
    p['bad']=[(x['id'],x['st']) for x in p['exports']+p['joins'] if x['st'][0] not in ('new','done')]
    print(p['label'],p['C'],round(p['T']/3600,2),'h',p['bad'][:2],[(e['id'],round(e['est'].get('Test',0)),e['st'][0]) for e in p['exports']])
PBU=[p for p in PBU if not p['bad']]
UNITS+=PBU
print('total est h',sum(u['T'] for u in UNITS)/3600)
# ---- fresh ids, bundles
REN={}
for u in UNITS:
    for e in u['exports']:
        if e['id'] not in REN: REN[e['id']]=fresh(e['id'])
        e['id2']=REN[e['id']]
    for j in u['joins']: j['id2']=fresh(j['id']); j['exports2']=[REN.get(x,x) for x in j['exports']]
seenj=set()
for u in UNITS: u['joins']=[j for j in u['joins'] if not (j['id2'] in seenj or seenj.add(j['id2']))]
par={}
def fnd(x):
    while par.setdefault(x,x)!=x: x=par[x]
    return x
for u in UNITS:
    ids=[e['id2'] for e in u['exports']]
    for a in ids[1:]: par[fnd(a)]=fnd(ids[0])
    fnd(ids[0])
BUN=collections.defaultdict(lambda: dict(units=[],exports={}))
for u in UNITS:
    b=BUN[fnd(u['exports'][0]['id2'])]; b['units'].append(u)
    for e in u['exports']: b['exports'][e['id2']]=e
for b in BUN.values():
    gm=max([e['est'].get('g',1) for e in b['exports'].values()]+[1])
    b['T']=sum(e['est'].get('Test',0) for e in b['exports'].values())+sum(max(j.get('T',0.1),0.1)*gm for u in b['units'] for j in u['joins'])
    b['M']=max([e['est'].get('Mest',0) for e in b['exports'].values()]+[0])
    b['cat']=min(u['cat'] for u in b['units']); b['allc']=any(u['allc'] for u in b['units']); b['B']=any(u['phase']=='B' for u in b['units'])
SLOTS=5; BUDGET=36/1.3*3600
load=[0.0]*SLOTS; placed=[]; skipped=[]
for b in sorted(BUN.values(),key=lambda b:(b['cat'],not b['allc'],not b['B'],b['T'])):
    if b['T']>BUDGET or b['M']>230*1024: skipped.append((b,'too big')); continue
    s=min(range(SLOTS),key=lambda i:load[i])
    if load[s]+b['T']>BUDGET: skipped.append((b,'no room')); continue
    load[s]+=b['T']; b['slot']=s; placed.append(b)
print('placed',len(placed),'load h',[round(x/3600,1) for x in load])
print('skipped',collections.Counter((x[1],x[0]['cat']) for x in skipped))
def blab(b): return ' | '.join(sorted(set(f"{u['label']} C={','.join(map(str,u['C']))}" for u in b['units'])))
def subnums(s,C):
    toks=re.split(r'([, _])',s); it=iter(C); n=sum(t.isdigit() for t in toks)
    if n!=len(C): return s
    return ''.join(str(next(it)) if t.isdigit() else t for t in toks)
def side_line(newid,tid,C,kind,model,extra=''):
    a=side_of(tid)
    if a is None:
        a={k:'' for k in HDR}; a.update(table='X' if newid.startswith('X') else 'C',family=newid.split('.')[1],model=model,engine='join' if kind=='join' else 'must-partitioned',threads='1' if kind=='join' else '16',kind='cross-language' if kind=='join' else 'export',mode='offline-batch',exploration='full',benchmark=newid.split('.')[1],size=','.join(map(str,C)))
    else:
        a['size']=subnums(a.get('size',''),C)
        for k in ('spec_case','impl_case'): a[k]=subnums(a[k],C) if a.get(k) else a.get(k,'')
    a['id']=newid; a['tier']='cinf'; a['note']=((a.get('note') or '')+' scaleup-1110-brain02'+extra).strip()
    return '\t'.join((a.get(k) or '') for k in HDR)
CATN={1:'further ladder step',2:'first step > 20 h',3:'formerly held (pending row in a dead rows file)'}
rows=[f"# scaleup-1110 brain02 (coordinator 2026-11-10, after 18:15): 5 slots on NUMA 0,2,5,6,7, reserved node; Offline only, tier cinf, mem 250, JMC -Xmx12g (JMC_MAX 3 in the dispatcher); "
      f"{len(placed)} groups, estimated {sum(load)/3600:.1f} h over 5 slots ({', '.join(f'{x/3600:.1f}' for x in load)} h); biggest groups first so they finish by 2026-11-12 10:00; exports before their joins. Plan: plan/eval2/SCALEUP-2026-11-10.md (brain02 section)."]
tsv=['\t'.join(HDR)]; IDS=[]
for b in sorted(placed,key=lambda b:-b['T']):
    rows.append(f"# group [{CATN[b['cat']]}; {'all sizes conform' if b['allc'] else 'has a violation'}; est {b['T']/3600:.2f} h]: "+blab(b))
    for eid,e in b['exports'].items():
        cmd=e['cmd']
        if 'jmc-run.sh' in cmd: cmd='env JAVA_TOOL_OPTIONS=-Xmx12g '+cmd
        rows.append(f"{eid} 250 8640000 {cmd}"); IDS.append(eid)
        uu=[u for u in b['units'] if any(x['id2']==eid for x in u['exports'])][0]
        tsv.append(side_line(eid,e['tid'],uu['C'],'export',(uu['fam']['k'][5].split('/')[0] if uu['fam'] else 'fifo'),' re-export' if e['st'][0]=='done' else ''))
    for u in b['units']:
        for j in u['joins']:
            a,bb=j['exports2']
            rows.append(f'{j["id2"]} 250 8640000 needok:{a},{bb} "$EVAL_OUT"/tf-merge-target/release/conf-join --merge "$EVAL_OUT"/{a}/export.jsonl "$EVAL_OUT"/{bb}/export.jsonl')
            tsv.append(side_line(j['id2'],j['tid'],u['C'],'join',(u['fam']['k'][5].split('/')[0] if u['fam'] else 'fifo'))); IDS.append(j['id2'])
open(SC+'brain02.rows','w').write('\n'.join(rows)+'\n'); open(SC+'brain02.rows.tsv','w').write('\n'.join(tsv)+'\n')
print('rows',len(IDS),'dups',[i for i,n in collections.Counter(IDS).items() if n>1][:3],'exact clashes',[i for i in IDS if i in ALLIDS][:3],'base clashes',[i for i in IDS if status_of(i)[0] in ('queued',) or status_of(i)[0].startswith('failed')][:3])
pickle.dump(dict(placed=[(blab(b),b['cat'],b['allc'],b['T'],b['M'],b['slot'],len(b['exports']),sum(len(u['joins']) for u in b['units']),[(u['label'],u['C'],u['k'],u['cat']) for u in b['units']]) for b in placed],
                 skipped=[(blab(b),w,b['cat'],b['allc'],b['T']) for b,w in skipped],load=load,n=len(IDS)),open('/tmp/su1110/b2.pkl','wb'))
