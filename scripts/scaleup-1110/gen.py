exec(open('/tmp/su1110/an.py').read())
out=[family_eval(k,v) for k,v in sorted(FAM.items())]
def measured_fix(exs):
    for e in exs:
        if e['st'][0]=='done':
            best=None
            for s_,i in e['st'][1]:
                if s_=='done':
                    r=rec_of(i); w=fnum(r['wall_ms']); 
                    if w is not None and (best is None or w>best[0]): best=(w/1000,(fnum(r['peak_rss_kb']) or 0)/1024,i,r['node'])
            if best: e['est']=dict(e['est'],T=best[0],Test=best[0],Mest=best[1],node=best[3],how='measured at this size (re-export)',gstep=f'{best[2]} {best[0]:.1f} s on {best[3]}')
for f in out:
    oks=[];bl=[]
    for c in f['cands']:
        if 'skip' in c: bl.append((c['C'],c['skip'])); continue
        measured_fix(c['exports'])
        s=cand_summary(f,c)
        if not s['blocked'] and not s['jdone'] and not s['errs']: oks.append((s['T'],c,s))
        else:
            why=[x['id']+': '+x['st'][0]+' ('+', '.join(i for _,i in x['st'][1])+')' for x in s['blocked']]+[j['id']+': '+j['st'][0] for j in s['jdone']]+[e['id']+': '+e['est']['err'] for e in s['errs']]
            bl.append((c['C'],'; '.join(why)))
    oks.sort(key=lambda x:x[0])
    # dominance: an export whose key failed at a size <= C (componentwise) with a limit (timeout, memory) cannot finish at C
    oks2=[]
    for T,c,s_ in oks:
        dom=None
        for e in c['exports']:
            k=exkey(e['tid'])
            if not k: continue
            for q,lst in EXDB[k[0]].items():
                if len(q)==len(c['C']) and all(q[j]<=c['C'][j] for j in range(len(q))):
                    for x in lst:
                        if x[2] in ('timeout','memory') or x[2].startswith('failed: incomplete'): dom=f"{x[3]}: {x[2]}"
        if dom: bl.append((c['C'],'a smaller or equal size of an export hit a limit: '+dom))
        else: oks2.append((T,c,s_))
    f['oks']=oks2; f['bl']=bl
ALLIDS=set(idx)|set(R)
def fresh(i):
    if i not in ALLIDS: return i
    for k in range(2,20):
        j=f"{i}.r{k}"
        if j not in ALLIDS: return j
def bf(node): return 2.0 if node and node.startswith('brain') else 1.0
# ---------------- Phase C: choose candidates
chosen=[]   # (family, cand, summary)
fam_rows=[]
for f in out:
    oks=f['oks']; pick=[]
    if f['k'][1]=='tikv-txn':   # scenario-coded sizes whose numbers are not ladders (G 7224 at X2,0,0,1 and 4 at X2,0,1,1); the TiKV study owns its plan
        f['why_none']='tikv-txn: the size numbers are scenario switches, not a ladder (X2,0,0,1 has 7224 graphs, X2,0,1,1 has 4); left to the TiKV study'
        oks=[]
    flat=[]
    for T,c,s in oks:
        if all(e['est'].get('g',1)<=1.0 and 'measured' not in e['est'].get('how','') for e in c['exports']):
            flat.append(c['C']); continue
        if s['M']>230*1024 or T>20*3600: continue
        if not pick: pick.append((c,s))
        elif len(pick)==1 and T<=12*3600 and c['C']!=pick[0][0]['C']: pick.append((c,s))
    f['pick']=pick
    f['flat']=locals().get('flat',[])
    for c,s in pick: chosen.append((f,c,s))
# second step along the same axis (C2 = C + e_axis) for a family whose first step is cheap (< 1 h on a brain node):
# ids from the base-size templates, estimate = first-step estimate x g once more
import copy
extra=[]
for f,c,s_ in chosen:
    if s_['T']>12*3600 or not c['exports']: continue
    C2=list(c['C']); C2[c['ax']]+=1; C2=tuple(C2)
    if C2 in f.get('S',{}): continue
    mx=AX.get(f['k'][1].replace('cf-',''),(None,None))[1]
    if mx and mx[c['ax']] is not None and C2[c['ax']]>mx[c['ax']]: continue
    c2=dict(P=c['P'],C=C2,ax=c['ax'],exports=[],joins=[],second=True); bad=False
    for e in c['exports']:
        nid=new_export_id(e['tid'],C2); cmd=sub_cmd(idx[e['tid']][0][3],C2) if e['tid'] in idx else None
        if not nid or not cmd: bad=True; break
        est=dict(e['est']); g=est.get('g',1)
        if 'Test' in est:
            est['Test']=est['Test']*g; est['Mest']=est.get('Mest',0)*min(g,max(1.0,est.get('Mest',1)/max(est.get('M',1),1)))
            est['how']=est.get('how','')+'; second step: x g again'
        c2['exports'].append(dict(tid=e['tid'],id=nid,cmd=cmd,st=status_of(nid),est=est))
    if bad: continue
    measured_fix(c2['exports'])
    for j in c['joins']:
        nid=new_join_id(j['tid'],C2)
        c2['joins'].append(dict(j,id=nid,exports=[new_export_id(x,C2) for x in [e for e in idx[j['tid']][0][3].split() if False]] or None,st=status_of(nid)))
    # join exports: map the first-step export ids to the second-step ones by position in c['exports']
    m={e1['id']:e2['id'] for e1,e2 in zip(c['exports'],c2['exports'])}
    for j1,j2 in zip(c['joins'],c2['joins']): j2['exports']=[m[x] for x in j1['exports']]
    s2=cand_summary(f,c2)
    if s2['blocked'] or s2['jdone'] or s2['errs'] or s2['T']>12*3600: continue
    dom=None
    for e in c2['exports']:
        k=exkey(e['tid'])
        for q,lst in EXDB[k[0]].items():
            if len(q)==len(C2) and all(q[jj]<=C2[jj] for jj in range(len(q))):
                for x in lst:
                    if x[2] in ('timeout','memory') or x[2].startswith('failed: incomplete'): dom=x
    if dom: continue
    extra.append((f,c2,s2)); f['pick'].append((c2,s2))
chosen+=extra
# ---------------- Phase B: hand-listed scale-ups from the realsw templates
PB=[]
def pb(fam_label, allc, P, C, exports, joins, axisname):
    """exports: list of template export ids at P; joins: list of (template join id or pattern id at P, [indices of spec export, impl export]) """
    ex=[]
    for t in exports:
        nid=new_export_id(t,C); cmd=sub_cmd(idx[t][0][3],C)
        ex.append(dict(tid=t,id=nid,cmd=cmd,st=status_of(nid),est=est_export(t,P,AXI[axisname])))
    js=[]
    for jt,(a,b),dirn,verdict in joins:
        nid=new_join_id(jt,C)
        js.append(dict(dirn=dirn,tid=jt,id=nid,exports=[ex[a]['id'],ex[b]['id']],st=status_of(nid),T=0.1,M=10,verdict=verdict))
    PB.append(dict(label=fam_label,allc=allc,P=P,C=C,exports=ex,joins=js,axis=axisname))
AXI={'proposals':0,'timeouts':1,'candidates':2}
# Phase B entries (templates = the measured rows of tab:eval2:realsw)
OM_S='C3E.omnipaxos.spec_correct_3_0_1_fifo.t16.cinf'; OM_I='C3E.omnipaxos.impl_correct_3_0_1_fifo.t16.cinf'
pb('OmniPaxos <= S_raft (TF/TF)',True,(3,0,1),(4,0,1),[OM_S,OM_I],[('C3J.omnipaxos.impl_correct_3_0_1_fifo.cinf.m',(0,1),'R>R','conforms')],'proposals')
pb('OmniPaxos <= S_raft (TF/TF)',True,(3,0,1),(5,0,1),[OM_S,OM_I],[('C3J.omnipaxos.impl_correct_3_0_1_fifo.cinf.m',(0,1),'R>R','conforms')],'proposals')
pb('OmniPaxos <= S_omni (TF/TF)',True,(2,0,1),(3,0,1),['C3E.omnipaxos-derived.spec_correct_2_0_1_fifo.t16.cinf','C3E.omnipaxos.impl_correct_2_0_1_fifo.t16.c48h'],[('C3J.omnipaxos.impl_correct_2_0_1_fifo.vs-derived.cinf.m',(0,1),'R>R','conforms')],'proposals')
for C,P,ax in (((1,3,1),(1,2,1),'timeouts'),((2,2,2),(2,2,1),'candidates'),((2,3,1),(2,2,1),'timeouts')):
    q='_'.join(map(str,P))
    pb('S_raft <= S_omni and S_omni <= S_raft (TF/TF)',True,P,C,
       [f'C3E.omnipaxos.spec_correct_{q}_fifo.t16.c48h' if P!=(1,2,1) else 'C3E.omnipaxos.spec_correct_1_2_1_fifo.t16.c48h',
        f'C3E.omnipaxos-derived.spec_correct_{q}_fifo.t16.c48h' if P!=(1,2,1) else 'C3E.omnipaxos-derived.spec_correct_1_2_1_fifo.t16.c48h.r4'],
       [(f'C3J.omnipaxos.spec_correct_{q}_fifo.vs-derived.c48h.m',(1,0),'S_raft<=S_omni','conforms'),(f'C3J.omnipaxos-derived.spec_correct_{q}_fifo.vs-hand.c48h.m',(0,1),'S_omni<=S_raft','violation')],ax)
pb('raft-rs <= S_omni (TF/TF) and raft-rs <= Java S_raft (TF/JMC)',False,(1,2,1),(1,3,1),
   ['C3E.raft-rs.impl_correct_1_2_1_fifo.t16.c48h.r2','C3E.omnipaxos-derived.spec_correct_1_2_1_fifo.t16.c48h.r4','XJE.raft-rs.spec_correct_1_2_1_fifo.w16.c48h'],
   [('C3J.raft-rs.impl_correct_1_2_1_fifo.vs-omni.cinf.m',(1,0),'raft-rs<=S_omni','conforms'),('XJ2.raft-rs.impl_correct_1_2_1_fifo.jmcspec-tfimpl.c48h.m',(2,0),'raft-rs<=S_raft(Java)','conforms')],'timeouts')
for p in PB: measured_fix(p['exports'])
PB_HELD=[p for p in PB if any(x['st'][0] not in ('new','done') for x in p['exports']+p['joins'])]
PB=[p for p in PB if p not in PB_HELD]
# the 1,3,1 raft-rs export was measured (8574 s brain01, 8258 s pinky10): use the measurement, not the extrapolation
for p in PB:
    for e in p['exports']:
        if e['id'].startswith('C3E.raft-rs.impl_correct_1_3_1'):
            m=ex_ok(exkey('C3E.raft-rs.impl_correct_1_3_1_fifo.t16.cinf')[0],(1,3,1)); e['est']=dict(T=m[0],M=m[1],node=m[4],tid=m[3],g=1,Test=m[0],Mest=m[1],gstep=f'measured at this size: {m[3]} {m[0]:.0f} s on {m[4]}',how='measured')
    if p['C']==(5,0,1):   # two steps of the (2,0,1)->(3,0,1) growth
        for e in p['exports']:
            if 'Test' in e['est']: e['est']['Test']*=e['est']['g']; e['est']['Gest']=e['est'].get('Gest',0)*e['est']['g']; e['est']['how']+=' twice (two steps)'
# S_raft in the omnipaxos directory: 24 graphs at (1,e,1); its (1,2,1) record (6436 s, load 102) is contended, so the
# (1,3,1) estimate uses the uncontended (1,1,1) time instead
for p in PB:
    for e in p['exports']:
        if e['id']=='C3E.omnipaxos.spec_correct_1_3_1_fifo.t16.cinf':
            e['est'].update(T=0.05,Test=0.05,how='S_raft has 24 graphs at (1,1,1) and (1,2,1); time of the uncontended (1,1,1) run',gstep='C3E.omnipaxos.spec_correct_1_1_1_fifo.t16.c48h G=24 0.05 s; (1,2,1) G=24')
# ---------------- units
UNITS=[]
for f,c,s in chosen:
    if not c['exports']: continue
    kind,d,prog,var,spec,model=f['k']
    lab=f"{kind} {prog} [{d}] {var} vs {var if spec=='=' else spec} {model}"
    UNITS.append(dict(label=lab,allc=f['allc'],fam=f,C=c['C'],P=c['P'],ax=c['ax'],exports=c['exports'],joins=c['joins'],phase='C'))
for p in PB:
    UNITS.append(dict(label=p['label'],allc=p['allc'],fam=None,C=p['C'],P=p['P'],ax=AXI[p['axis']],axisname=p['axis'],exports=p['exports'],joins=p['joins'],phase='B'))
# fresh ids (an exact id that exists already gets the next free .rN) and the export map
EXMAP={}   # new export id -> export dict
REN={}
for u in UNITS:
    for e in u['exports']:
        if e['id'] in REN: e['id2']=REN[e['id']]
        else:
            e['id2']=fresh(e['id']); REN[e['id']]=e['id2']
        EXMAP.setdefault(e['id2'],e)
    for j in u['joins']:
        j['id2']=fresh(j['id']); j['exports2']=[REN.get(x,x) for x in j['exports']]
# drop units whose joins are duplicated (same join id from two units)
seenj=set()
for u in UNITS:
    u['joins']=[j for j in u['joins'] if not (j['id2'] in seenj or seenj.add(j['id2']))]
# bundles: union-find over export ids
par={}
def fnd(x):
    while par.setdefault(x,x)!=x: x=par[x]
    return x
for u in UNITS:
    ids=[e['id2'] for e in u['exports']]
    for a in ids[1:]: par[fnd(a)]=fnd(ids[0])
    fnd(ids[0])
BUN=collections.defaultdict(lambda: dict(units=[],exports={},joins=[]))
for u in UNITS:
    b=BUN[fnd(u['exports'][0]['id2'])]; b['units'].append(u)
    for e in u['exports']: b['exports'][e['id2']]=e
    b['joins']+=u['joins']
def cost(b,cls):
    t=0
    for e in b['exports'].values():
        x=e['est'].get('Test',0); n=e['est'].get('node','')
        if cls!='brain': x*=bf(n)
        if cls=='pinky-small': x*=2
        t+=x
    gm=max([e['est'].get('g',1) for e in b['exports'].values()]+[1])
    t+=sum(max(j.get('T',0.1),0.1)*gm for j in b['joins'])
    return t
for k,b in BUN.items():
    b['allc']=any(u['allc'] for u in b['units']); b['phase']='B' if any(u['phase']=='B' for u in b['units']) else 'C'
    b['M']=max([e['est'].get('Mest',0) for e in b['exports'].values()]+[0])
    b['Tb']=cost(b,'brain'); b['Tv']=cost(b,'volta')
print('units',len(UNITS),'bundles',len(BUN),'exports',len(EXMAP),'joins',sum(len(u['joins']) for u in UNITS))
print('total est brain h',sum(b['Tb'] for b in BUN.values())/3600,'volta h',sum(b['Tv'] for b in BUN.values())/3600)
# ---------------- placement
HOSTS=[]
for h in ['volta03','volta04','volta05','volta07','volta09','volta10','volta12','volta13','volta15']: HOSTS.append(dict(h=h,cls='volta',slots=1,win=42,mem=200,xmx='24g',memcap=180))
for h in ['pinky26','pinky27','pinky28','pinky29','pinky30','pinky32']: HOSTS.append(dict(h=h,cls='pinky-small' if h in ('pinky30','pinky32') else 'pinky',slots=1,win=42,mem=80,xmx='8g',memcap=60))
HOSTS.append(dict(h='brain09',cls='brain',slots=4,win=36,mem=250,xmx='24g',memcap=230))
HOSTS.append(dict(h='brain08',cls='brain',slots=1,win=42,mem=250,xmx='24g',memcap=230))
for H in HOSTS: H['budget']=(H['win']-2)/1.5*3600; H['load']=[0.0]*H['slots']; H['bundles']=[]
def hcost(b,H): return cost(b,'brain' if H['cls']=='brain' else ('pinky-small' if H['cls']=='pinky-small' else 'volta'))
order=sorted(BUN.values(),key=lambda b:(not b['allc'],-b['Tb']))
unplaced=[]
for b in order:
    big=b['Tb']>4*3600
    opts=[]
    for H in HOSTS:
        if b['M']>H['memcap']*1024: continue
        t=hcost(b,H); sl=min(range(H['slots']),key=lambda i:H['load'][i])
        if H['load'][sl]+t>H['budget']: continue
        pref=(0 if H['cls']=='brain' else 1) if big else (0 if H['cls']!='brain' else 2)
        opts.append((pref,H['load'][sl]/H['budget'],H['h'],H,sl,t))
    if not opts: unplaced.append(b); continue
    opts.sort(key=lambda x:x[:3]); _,_,_,H,sl,t=opts[0]
    H['load'][sl]+=t; H['bundles'].append((b,t,sl))
for H in HOSTS: print(H['h'],len(H['bundles']),[round(x/3600,1) for x in H['load']])
print('unplaced',[(b['units'][0]['label'],round(b['Tb']/3600,1),round(b['M']/1024)) for b in unplaced])
# ---------------- emission
import os
OUT=G+'daavar/cluster/campaign/scaleup-1110/'
def subnums(s,C):
    toks=re.split(r'([, _])',s); it=iter(C); n=sum(t.isdigit() for t in toks)
    if n!=len(C): return s
    return ''.join(str(next(it)) if t.isdigit() else t for t in toks)
def side_line(newid,tid,C,kind,u,extra=''):
    a=side_of(tid)
    if a is None:
        a={k:'' for k in HDR}; a.update(table='X' if newid.startswith('X') else 'C',family=newid.split('.')[1],model=u['fam']['k'][5].split('/')[0] if u['fam'] else 'fifo',
            engine='join' if kind=='join' else ('must-split' if newid.startswith(('XCE','XJE')) else 'must-partitioned'),threads='1' if kind=='join' else '16',
            kind='cross-language' if kind=='join' else 'export',mode='offline-batch',exploration='full',benchmark=newid.split('.')[1])
        a['size']=','.join(map(str,C))
    else:
        a['size']=subnums(a.get('size',''),C)
        for k in ('spec_case','impl_case'): a[k]=subnums(a.get(k,'') or '',C) if a.get(k) else a.get(k,'')
    a['id']=newid; a['tier']='cinf'
    a['note']=((a.get('note') or '')+' scaleup-1110'+extra).strip()
    return '\t'.join((a.get(k) or '') for k in HDR)
os.makedirs(OUT,exist_ok=True)
ALLROWIDS=[]
for H in HOSTS:
    if not H['bundles']: continue
    rows=[]; tsv=['\t'.join(HDR)]
    bl=sorted(H['bundles'],key=lambda x:(not x[0]['allc'],x[0]['phase']!='B',x[1]))
    tot=sum(t for _,t,_ in bl)
    rows.append(f"# scaleup-1110 (owner 2026-11-10 17:30): next sizes of Phase B (real software) and Phase C (twins), Offline only, tier cinf (no time limit), "
                f"memory {H['mem']} GB; generated by the scale-up agent, plan/eval2/SCALEUP-2026-11-10.md. {len(bl)} groups, estimated {tot/3600:.1f} h of work over {H['slots']} slot(s) "
                f"(extrapolated, see the plan); exports before their joins; families that conformed at every size first.")
    for b,t,sl in bl:
        labs=sorted(set(f"{u['label']} C={','.join(map(str,u['C']))}" for u in b['units']))
        rows.append(f"# group ({'all sizes conform' if b['allc'] else 'has a violation'}; est {t/3600:.2f} h on {H['cls']}): "+' | '.join(labs))
        u0=b['units'][0]
        for eid,e in b['exports'].items():
            cmd=e['cmd']
            if 'jmc-run.sh' in cmd: cmd=f"env JAVA_TOOL_OPTIONS=-Xmx{H['xmx']} "+cmd
            rows.append(f"{eid} {H['mem']} 8640000 {cmd}")
            uu=[u for u in b['units'] if any(x['id2']==eid for x in u['exports'])][0]
            tsv.append(side_line(eid,e['tid'],uu['C'],'export',uu, ' re-export' if e['st'][0]=='done' else ''))
            ALLROWIDS.append(eid)
        for u in b['units']:
            for j in u['joins']:
                a,bb=j['exports2']
                rows.append(f'{j["id2"]} {H["mem"]} 8640000 needok:{a},{bb} "$EVAL_OUT"/tf-merge-target/release/conf-join --merge "$EVAL_OUT"/{a}/export.jsonl "$EVAL_OUT"/{bb}/export.jsonl')
                tsv.append(side_line(j['id2'],j['tid'],u['C'],'join',u)); ALLROWIDS.append(j['id2'])
    open(OUT+H['h']+'.rows','w').write('\n'.join(rows)+'\n'); open(OUT+H['h']+'.rows.tsv','w').write('\n'.join(tsv)+'\n')
    H['nrows']=sum(1 for r in rows if not r.startswith('#'))
import collections as _c
dup=[i for i,n in _c.Counter(ALLROWIDS).items() if n>1]
print('rows',len(ALLROWIDS),'dup ids',dup[:5])
clash=[i for i in ALLROWIDS if i in ALLIDS]; print('exact clashes with existing ids',clash[:5])
# base2 clashes with live (queued) rows
live=[i for i in ALLROWIDS if status_of(i)[0]=='queued']; print('queued-base clashes',live[:5])
pickle.dump(dict(HOSTS=[{k:v for k,v in H.items() if k!='bundles'} for H in HOSTS]),open('/tmp/su1110/hosts.pkl','wb'))
