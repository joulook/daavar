exec(open('/tmp/su1110/gen.py').read())
L=[]
def fmt_v(d):
    a=d.get('J>R',{}).get('verdict',''); b=d.get('R>J',{}).get('verdict','')
    c=lambda v:'C' if v=='conforms' else ('V' if v=='violation' else '-')
    return c(a)+c(b)
# coarse-base collisions (cluster-pending.sh sorts the descriptor tokens)
live_bases={}
for i in idx:
    if rec_of(i) is None and not all(x[0].startswith(STALE) for x in idx[i]): live_bases.setdefault(base(i),i)
coarse=[(i,live_bases[base(i)]) for i in ALLROWIDS if base(i) in live_bases]
open('/tmp/su1110/coarse.txt','w').write('\n'.join(f'{a} ~ {b}' for a,b in coarse))
inv=[]
for f in out:
    k=f['k']; v=FAM[k]; ss=f['sizes']
    big=max(ss,key=lambda s:max(fnum(x.get('wall s')) or 0 for x in v[s].values()))
    W=max(fnum(x.get('wall s')) or 0 for x in v[big].values()); M=max(fnum(x.get('peak MB')) or 0 for x in v[big].values())
    ids=' '.join(x['run id'] for x in v[big].values())
    inv.append((f,big,W,M,ids))
pickle.dump(dict(inv=[(f['k'],f['sizes'],big,W,M,ids,f['allc'],[(c['C'],c['ax'],s['T'],s['M'],[e['est'].get('gstep','') for e in c['exports']],[e['est'].get('how','') for e in c['exports']],c.get('second',False)) for c,s in f['pick']],f['bl'],f.get('flat',[]),f.get('why_none'),[(T,c['C']) for T,c,s in f['oks']]) for f,big,W,M,ids in inv],
                 PB_HELD=[(p['label'],p['C'],[(x['id'],x['st']) for x in p['exports']+p['joins'] if x['st'][0] not in ('new','done')]) for p in PB_HELD],
                 PB=[(p['label'],p['allc'],p['P'],p['C'],p['axis'],[(e['id2'],e['st'][0],e['est']) for e in p['exports']],[j['id2'] for j in p['joins']]) for p in PB],
                 hosts=[(H['h'],H['cls'],H['slots'],H['win'],H['mem'],H['xmx'],H.get('nrows',0),H['load'],[(sorted(set(u['label']+' C='+','.join(map(str,u['C'])) for u in b['units'])),t,b['allc'],len(b['exports']),sum(len(u['joins']) for u in b['units'])) for b,t,sl in H['bundles']]) for H in HOSTS],
                 coarse=coarse, fam=None),open('/tmp/su1110/doc.pkl','wb'))
print('coarse',len(coarse)); print(coarse[:10])
