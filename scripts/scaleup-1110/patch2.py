s=open('/tmp/su1110/gen.py').read()
anchor="# ---------------- Phase B: hand-listed"
add='''# second step along the same axis (C2 = C + e_axis) for a family whose first step is cheap (< 1 h on a brain node):
# ids from the base-size templates, estimate = first-step estimate x g once more
import copy
extra=[]
for f,c,s_ in chosen:
    if s_['T']>3600 or not c['exports']: continue
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
'''
s=s.replace(anchor,add+anchor)
open('/tmp/su1110/gen.py','w').write(s)
