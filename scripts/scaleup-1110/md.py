import pickle,re
d=pickle.load(open('/tmp/su1110/doc.pkl','rb'))
exec(open('/tmp/su1110/an.py').read())
O=[]; w=O.append
def vcode(v): return {'conforms':'C','violation':'V'}.get(v,'-')
w("# Scale-up of Phase B and Phase C, 2026-11-10 (owner's order 17:30)")
w("")
w("Status: proposal and rows files only. Nothing was started, copied or queued on any node. Rows: `daavar/cluster/campaign/scaleup-1110/<host>.rows` (+ `.rows.tsv`), 532 rows on 17 hosts. `cluster/campaign/shared/` was not touched.")
w("")
w("## Sources (snapshot 2026-11-10 17:03 CET)")
w("- Twin CSVs (written 16:57): `PLDI/tex/tables/campaign-twin-offline-{java,rust}-spec.csv` (XT1/XT2, the Java ports) and `campaign-cftwin-offline-{java,rust}-spec.csv` (XC1/XC2, the context-free and case-study ports). Columns used: program, variant, size, model, verdict, wall s, peak MB, run id, node. The verdicts and the times of the inventory are read from these files.")
w("- Phase B: `PLDI/tex/tables/eval2/realsw.tex`, `standalone-realsw-{conf,viol}-full.csv`, `cross-realsw-{conf,viol}-full.csv`. These files carry no run ids. The ids below come from brain-data.")
w("- brain-data `results/*.tsv` at 518dc6761 (2026-11-10 16:57), 20 681 records. The export times, graph counts (`iterations`) and failures come from these records.")
w("- Every `*.rows` file under `daavar/cluster/campaign/` (27 241 ids), used for the templates (commands) and for the duplicate check. Every `*.rows.tsv`, used for the sidecar attributes.")
w("- `inputs/grid-axes.tsv` (structural axes, program bounds: paxos-oopsla25 and memcached-oopsla25 stop at 3), `inputs/exploratory-variants.tsv` (raft-rs reorder is left out), `paper-sizes.tsv` (not needed: no row here is a paper Tables 1-2 row).")
w("")
w("## Assumptions (please check)")
w("1. **Scope.** Phase B is every pair of tab:eval2:realsw, plus the cross-library checks at e=0 (raft-rs/MicroRaft vs OmniPaxos), which are in the cftwin CSV. Phase C is every family of the four offline twin CSVs. Java-vs-Java pairs (`jimpl-jimpl`, e.g. the ZooKeeper Opus port against the transliteration) are left out because they do not cross languages. tikv-txn is left out too: its size numbers are scenario switches, not a ladder (XC rows: X2,0,0,1 has 7224 graphs and X2,0,1,1 has 4), and the TiKV study has its own rows on brain17/18/23.")
w("2. **Family.** A family is (kind, directory, program, variant, spec side, model), plus the scenario words of the size (scalardb `lost/skew/mixed`, chainpaxos `mixed`). The two directions are kept on one line: J>R = XT1/XC1 (Java program as the reference, Rust program as the candidate) and R>J = XT2/XC2. `spec` `=` means the variant against its own twin; otherwise it is the `vs-` tag of the join id.")
w("3. **Duplicate rule.** A proposed id is exactly new: it is in no rows file and in no brain-data record. An instance counts as the same when the words are sorted, the numbers are kept in order, and the tier/rerun suffixes are dropped. A proposed instance is blocked when the same instance (a) has a brain-data record that hit a limit (timeout, memory, heap), or (b) is in a rows file without any record. Case (b) is counted as live unless every file holding it is a stale generator output from 11-01..11-03 (`phase1/2/3`, `scaling`, `scaling-cf`, `frontier`, `cf2`, `twins*`, `scaling-next`). I could not see the live queues (no ssh, by order). **Run `scripts/cluster-pending.sh` before starting.** It sorts all descriptor tokens, so it will report 3 false pairs: the new `1_3_1` S_omni export and joins against the queued `3_1_1` ones (`C3E/C3J.omnipaxos(-derived).spec_correct_3_1_1_fifo...c48h`). These are different sizes.")
w("4. **Re-exports.** A join reads both exports from the node's own `$EVAL_OUT`. Where the export at the new size was already done on another node (mostly the Phase A Rust exports on brain01-05), the rows re-run it under a fresh id on the target host. This follows the brain08 precedent of REBALANCE-2026-11-10 (\"the 19 exports they read go in too\"). 51 of the 223 exports are such re-exports, and their sidecar note says `re-export`. Importing the brain-data `export.jsonl` into `$EVAL_OUT/imports/` (the realsw-x-brain15 precedent) would save that time, but it is a manual step on the node, so I did not use it.")
w("5. **Start times.** The volta/pinky hosts and brain08 start at about 18:00 on 11-10, which leaves 42 h to the deadline. brain09 starts at 00:00 on 11-11 (NUMA 0-3 free), which leaves 36 h.")
w("")
w("## Method")
w("- **Candidates.** For every maximal measured size P of a family and every size axis k, the candidate is P+e_k (\"increment parameter k by one\"). Axes marked structural in grid-axes.tsv are skipped (raft-rs and omnipaxos: the third argument; Phase B handles it separately), and so are program bounds. A second step P+2e_k is added when its estimate is at most 12 h. An axis whose last measured step left the graph count unchanged (g = 1, e.g. the third argument of pbkv-sh from 3,3,2 to 3,3,3) is not extended, because that parameter does not change the program there.")
w("- **Cost model (extrapolation; planning only, never a table value, R10).** Each export E of the new size C is estimated separately. Let g be the graph-count ratio (brain-data `iterations`) of the closest measured step of the same export along the same axis, preferably the step that ends at P. If that export has no such step, the twin export in the other language is used, then the largest step on another axis. If the step before that had a smaller ratio, the growth is accelerating, and g is multiplied by the ratio of the two ratios. Then T(C) = T(P)·g when T(P) ≥ 30 s. Otherwise T(C) = T(P) + r·G(P)·(g−1), where r is the per-graph time of the same export's smallest-rate run of at least 30 s and 1000 graphs. When the export at C was already measured, its measured time is used. Memory: TraceForge M(P)·min(g, measured memory ratio); JMC the same when M(P) > 500 MB, else M(P)·g^0.6. Joins: join time at P · g. On volta/pinky a time measured on a brain node counts double, and pinky30/32 (8 cores in all) count double again.")
w("- **Feasible.** Estimate ≤ 20 h on a brain node and estimated peak ≤ 230 GB. An export whose instance hit the 48 h limit at a size ≤ C, coordinate by coordinate, makes C infeasible (e.g. MicroRaft (2,1,1) → (2,2,1)).")
w("- **Selection.** Per family, the cheapest feasible candidate, plus a second one on another axis when it is ≤ 12 h, plus the second steps above. Families that shared an export were put in one group, so they go to one host. Groups that conformed everywhere are placed first, the biggest on brain nodes. A host takes groups while its estimated work stays under (window − 2 h)/1.5: 26.7 h on volta/pinky/brain08 and 22.7 h per slot on brain09. Memory caps: pinky 60 GB, volta 180 GB, brain 230 GB.")
w("")
# ---------------- (a) inventory
w("## (a) Inventory")
w("")
w("### Phase B: real software (tab:eval2:realsw; ids from brain-data)")
w("")
w("| pair (cand ⊑ ref), tools | sizes: verdict | largest size: T s, M MB (record) | every size conforms |")
w("|---|---|---|---|")
PBI=[
("S_raft ⊑ S_omni, TF/TF","1,1,1 C; 2,1,1 C; 1,2,1 C; 2,1,2 C; 2,2,1 C","2,2,1: S_omni export 507.7 s, 20 MB on pinky07 (C3E.omnipaxos-derived.spec_correct_2_2_1_fifo.t16.c48h); join C3J.omnipaxos.spec_correct_2_2_1_fifo.vs-derived.c48h.m","yes"),
("S_omni ⊑ S_raft, TF/TF","the same five sizes, V at each (C3J.omnipaxos-derived.spec_correct_*.vs-hand…)","2,2,1 as above","no"),
("raft-rs ⊑ S_raft, TF/TF","1,1,1 C; 1,2,1 C; 1,3,1 C; 1,1,2 V (4 uncovered); 2,1,1 none (C3E.raft-rs.impl_correct_2_1_1 exit-101 on brain01; reorder variant timed out at 48 h on brain13)","1,3,1: 8574 s, 792 MB on brain01 (C3E.raft-rs.impl_correct_1_3_1_fifo.t16.cinf, 186 603 863 graphs; join C3J.raft-rs.impl_correct_1_3_1_fifo.cinf.m C)","no"),
("raft-rs ⊑ S_omni, TF/TF","1,1,1 C; 1,2,1 C; 1,1,2 V (C3J.raft-rs.impl_correct_*.vs-omni.cinf.m, brain15)","1,1,2: raft-rs export 9029 s / 10931 s, 1.9 GB","no"),
("raft-rs ⊑ S_raft (Java), TF/JMC","1,1,1 C; 1,2,1 C; 1,1,2 V (XJ2.raft-rs.impl_correct_*.jmcspec-tfimpl, brain13)","1,1,2 as above","no"),
("OmniPaxos ⊑ S_raft, TF/TF","1,0,1 C; 1,0,2 C; 2,0,1 C; 2,0,2 C; 3,0,1 C; 1,1,1 none (C3E.omnipaxos.impl_correct_1_1_1_fifo.t16.c48h timeout 48 h, brain05)","3,0,1: 255.9 s, 30 MB, 6 899 200 graphs (C3E.omnipaxos.impl_correct_3_0_1_fifo.t16.cinf, brain01; C3J.omnipaxos.impl_correct_3_0_1_fifo.cinf.m C)","yes (e = 0 only)"),
("OmniPaxos ⊑ S_omni, TF/TF","1,0,1 C; 1,0,2 C; 2,0,1 C (C3J.omnipaxos.impl_correct_*.vs-derived.cinf.m, brain15)","2,0,1: 38.4 s, 21 MB (OmniPaxos export); S_omni 2.3 s","yes"),
("MicroRaft ⊑ S_raft, JMC/TF","1,1,1 C; 1,2,1 C; 2,1,1 and 1,1,2 none (XJE.raft-rs.impl_correct_{2_1_1,1_1_2}_fifo.w16.c48h timeout 48 h, brain01)","1,2,1: 40 041 s, 72 117 MB, 6 289 273 graphs (XJE.raft-rs.impl_correct_1_2_1_fifo.w16.c48h, brain01)","yes"),
("MicroRaft ⊑ S_omni, JMC/TF","1,1,1 C; 1,2,1 C (XJ1.raft-rs.impl_correct_*.vs-omni…)","1,2,1 as above","yes"),
("raft-rs ⊑ MicroRaft and MicroRaft ⊑ raft-rs","1,1,1 CC; 1,2,1 CC (XT2/XT1.raft-rs.impl_correct_*; twin CSVs)","1,2,1 as above","yes"),
("raft-rs/MicroRaft vs OmniPaxos at e = 0 (C3J vs-omnipaxos / vs-raft-rs, XC1.raft-rs…vs-omnipaxos, XC2.omnipaxos…vs-microraft)","1,0,1 C; 1,0,2 C (TF/TF only); 2,0,1 C, all directions","2,0,1: OmniPaxos 38.4 s; raft-rs 9 graphs, MicroRaft 1 graph at e = 0","yes"),
]
for r in PBI: w("| "+" | ".join(r)+" |")
w("")
w("### Phase C: twins (one line per family; verdict pairs are J>R R>J, `-` = no row in that direction)")
w("")
w("| # | kind | program [directory] | variant vs spec side | model | sizes: J>R R>J | largest: T s, M MB | all conform | CSV run ids at the largest size |")
w("|---|---|---|---|---|---|---|---|---|")
for n,(k,sizes,big,W,M,ids,allc,pick,bl,flat,whyn,oks) in enumerate(sorted(d['inv'],key=lambda x:(not x[6],x[0][2],x[0][1],x[0][3],x[0][4],x[0][5],x[0][0])),1):
    v=FAM[k]
    sv=' '.join(s.replace(' ','~')+':'+''.join(vcode(v[s].get(dd,{}).get('verdict','')) for dd in ('J>R','R>J')) for s in sizes)
    w(f"| {n} | {k[0]} | {k[2]} [{k[1]}] | {k[3]} vs {k[3] if k[4]=='=' else k[4]} | {k[5]} | {sv} | {big.replace(' ','~')}: {W:g}, {M:g} | {'yes' if allc else 'no'} | {ids} |")
w("")
# ---------------- (b) proposal
w("## (b) Proposal")
w("")
w("Estimates are extrapolations (method above), not measurements. Graph ratios are given as `from → to (ratio)` with the brain-data ids of the step.")
w("")
w("### Phase B (all in the rows files except where noted)")
w("")
w("| pair | next size (parameter) | new exports: id, estimate | joins |")
w("|---|---|---|---|")
for lab,allc,P,C,ax,exs,js in d['PB']:
    pname={'proposals':'proposals per client +1','timeouts':'election timeouts +1','candidates':'election candidates (nodes allowed to time out) +1'}[ax]
    ex='; '.join(f"`{i}` ({'re-export, ' if st=='done' else ''}{e.get('Test',0):.0f} s, {e.get('how','')}; {e.get('gstep','')})" for i,st,e in exs)
    w(f"| {lab} | {','.join(map(str,C))} from {','.join(map(str,P))} ({pname}) | {ex} | {'; '.join('`'+j+'`' for j in js)} |")
for lab,C,why in d['PB_HELD']:
    w(f"| {lab} | {','.join(map(str,C))} | **held**: {'; '.join(i+' '+s[0]+' ('+', '.join(x for _,x in s[1])+')' for i,s in why)}. The (3,0,1) OmniPaxos export is also listed, without a record, in `reruns/realsw-next-brain01.rows`, which looks superseded by the `.cinf` file. If the coordinator confirms that file is not in a live queue, add C3E.omnipaxos-derived.spec_correct_3_0_1_fifo.t16.cinf (est. 2076 s), a re-export of the OmniPaxos (3,0,1) export (256 s measured) and C3J.omnipaxos.impl_correct_3_0_1_fifo.vs-derived.cinf.m. | |")
w("")
w("Phase B sizes that cannot finish by 11-12 12:00 (no rows):")
w("- raft-rs (1,4,1): the step (1,2,1)→(1,3,1) is 12 486 268 → 186 603 863 graphs (14.9×), and the step before that was 9.4×. Extrapolated g ≈ 23.6, so 8574 s × 23.6 ≈ 202 000 s (56 h). raft-rs (2,1,1): the export did not finish in 48 h (reorder on brain13; the correct variant failed with exit-101 on brain01). raft-rs (1,1,3): going from 1 to 2 candidates multiplied the graphs by 370 (1 323 550 → 490 618 796), so 3 candidates is out of reach.")
w("- MicroRaft (1,3,1): (1,1,1)→(1,2,1) is 483 541 → 6 289 273 graphs (13×) at 40 041 s, so the estimate is at least 520 000 s. (2,1,1) and (1,1,2) reached the 48 h limit. Every MicroRaft pair, including raft-rs ⊑ MicroRaft and its converse, therefore stays at (1,2,1).")
w("- OmniPaxos (1,1,1): 48 h timeout. S_omni (4,0,1): the step (1,0,1)→(2,0,1) is 62 → 42 198 graphs (680×), and (3,0,1) is not measured, so the estimate is far above the window. S_omni (3,1,1): already in a rows file without a record (`C3E.omnipaxos-derived.spec_correct_3_1_1_fifo.t16.c48h`).")
w("- Participants: the harness fixes two clients and three nodes (raft-rs/README.md, \"three Raft nodes … two visible clients\"; caption of tab:eval2:realsw). The only participant parameter is cand ≤ 3, and its increment is proposed only for the specification pair (2,2,2).")
w("")
def plist(allc):
    rows=[]
    for k,sizes,big,W,M,ids,a,pick,bl,flat,whyn,oks in d['inv']:
        if a!=allc or not pick: continue
        for C,ax,T,Mm,gsteps,hows,second in pick:
            gs='; '.join(sorted(set(g for g in gsteps if g)))
            rows.append((k,C,ax,T,Mm,gs,second))
    return rows
for allc,title in ((True,"### Phase C families that conformed at every size (priority)"),(False,"### Phase C families with a violation at some size")):
    w(title); w("")
    w("| program [dir] variant vs spec, model, kind | next size (axis k = size argument k, +1) | est. T (brain) | est. peak | extrapolation basis |")
    w("|---|---|---|---|---|")
    for k,C,ax,T,Mm,gs,second in sorted(plist(allc),key=lambda r:(r[0][2],r[0][1],r[0][3],r[0][4],r[0][5],r[1])):
        w(f"| {k[2]} [{k[1]}] {k[3]} vs {k[3] if k[4]=='=' else k[4]}, {k[5]}, {k[0]} | {','.join(map(str,C))} (axis {ax+1}{', second step' if second else ''}) | {T/3600:.2f} h | {Mm/1024:.1f} GB | {gs} |")
    w("")
w("### Families not scaled, and why")
w("")
w("| program [dir] variant vs spec, model, kind | all conform | reason |")
w("|---|---|---|")
for k,sizes,big,W,M,ids,a,pick,bl,flat,whyn,oks in sorted(d['inv'],key=lambda x:(not x[6],x[0][2],x[0][1])):
    if pick: continue
    reasons=[]
    if whyn: reasons.append(whyn)
    if oks: reasons.append('next size estimated above the 20 h / 230 GB limit: '+', '.join(f"{','.join(map(str,C))} ≈ {T/3600:.0f} h" for T,C in oks[:2]))
    if flat: reasons.append('axis does not grow the graph count: '+', '.join(','.join(map(str,c)) for c in flat))
    for C,why in bl[:3]:
        why=re.sub(r'\s+',' ',why)
        reasons.append(f"{','.join(map(str,C))}: {why[:260]}")
    if not reasons: reasons.append('no numeric size axis or every axis structural/bounded')
    w(f"| {k[2]} [{k[1]}] {k[3]} vs {k[3] if k[4]=='=' else k[4]}, {k[5]}, {k[0]} | {'yes' if a else 'no'} | {' / '.join(reasons)} |")
w("")
# ---------------- (c) placement
w("## (c) Placement")
w("")
w("Files: `daavar/cluster/campaign/scaleup-1110/<host>.rows` + `.rows.tsv`. To start one through the queue watch's self-heal, copy it to `cluster/campaign/shared/reserve-<host>.rows` (+ `.rows.tsv`) (this is the coordinator's step; I did not do it). Dispatcher environment as in the precedents: FOREIGN_LOAD_POLICY=ignore, NUMA_SET=0 (volta/pinky) or 0,1,2,3 (brain09, only after 23:59), MEM_CAP 200 (volta), 80 (pinky), 250 (brain). The memory column of every row is set to its host class. JMC exports carry `env JAVA_TOOL_OPTIONS=-Xmx24g` (volta, brain) or `-Xmx8g` (pinky), as in rebalance-1110/{volta13,brain08,pinky29}.rows. Every row is `.cinf` (timeout 8640000). In each file, exports come before their joins, and families that conformed everywhere come first, cheapest first.")
w("")
w("| host | rows | groups | slots | est. work (h, per slot) | window | contents (group: families, size) |")
w("|---|---|---|---|---|---|---|")
for h,cls,slots,win,mem,xmx,nrows,load,bundles in d['hosts']:
    if not nrows: continue
    cont='<br>'.join(f"[{'C' if allc else 'V'} {t/3600:.1f} h, {ne} exp + {nj} joins] "+' / '.join(labs) for labs,t,allc,ne,nj in sorted(bundles,key=lambda b:(not b[2],b[1])))
    w(f"| {h} | {nrows} | {len(bundles)} | {slots} | {', '.join(f'{x/3600:.1f}' for x in load)} | {win} h | {cont} |")
w("")
w("Totals: 532 rows (223 exports, 309 joins), 66 groups, 86 families (64 that conformed everywhere) plus 6 Phase B entries. The estimated work is about 116 h on brain nodes, or about 192 h if all of it ran on volta. That is enough for 3-10 h per volta/pinky host and 13-18 h per brain slot. **The nodes will run out again around 11-11 02:00-08:00 (volta/pinky).** Every other next size is blocked: already queued or recorded, past the 48 h limit, or estimated beyond the window (tables above). The free capacity left (about 20 h × 15 volta/pinky hosts) could take third steps of the cheap ladders, or the held and queued sizes once the coordinator confirms that their rows files are dead.")
w("")
w("## Top five candidates where a bigger size could show a violation (my reading of the measured rows, not a prediction of verdicts)")
TOP=[
"1. **S_raft ⊑ S_omni at (2,2,2) and (2,3,1)** (Phase B; volta04 and volta05). It conformed at all five sizes, while the converse violates at all five (1448 uncovered at 2,1,1 per eval2.tex). (2,2,2) is the first increment of the candidates parameter for the specification pair. Estimate ≈ 1.6 h each (S_omni export, g = 11.7 from C3E.omnipaxos-derived.spec_correct_2_1_1 → 2_1_2).",
"2. **cassandra-rr `none`, Java port vs Rust port, 3 commands per client** (XC2.cassandra-rr.impl_3_none_fifo.vs-rust.tfimpl-jimpl.cinf.m; volta15). It conformed at 1 and 2 (measured in the R>J direction only, XC2), while the same Java port against the other Rust port (rust-sol) conformed at 1 in both directions and violated at 2 with the Java port as the reference (XC1.cassandra-rr.impl_2_none_fifo.vs-rust-sol.jimpl-tfimpl.c48h.m; cftwin CSV). Estimate ≈ 0.8 h.",
"3. **replicated-log-aneris `asyn-fault` twins, fifo, W = 6 and 7** (XT/XC). The twins conformed at 2-5, while `asyn-fault` against `correct` conformed at 2 and violated from 3 on. In this program the fault shows only from some size on. Estimate 0.3 h (6) and 4.3 h (7); brain09 and volta.",
"4. **chainpaxos-code-sol `mixed` and `local` twins, 3-5 commands** (the Codex Java/Rust ports from ChainPaxos's source). They conformed at 1-3 in both directions, while the Opus pair from the same source violates in the J>R direction (XC1, Java as the reference) from 1 (`mixed`) and from 2 (`local`). Each is cheap (< 0.1 h); volta04.",
"5. **OmniPaxos ⊑ S_raft at (4,0,1) and (5,0,1)**, with the e = 0 cross-library pairs OmniPaxos ⊑ MicroRaft and MicroRaft ⊑ OmniPaxos at (3,0,1) and (4,0,1). Phase B, conformed at every measured size. More proposals per client is the only axis OmniPaxos can still climb (e = 1 timed out at 48 h). Estimates 1820 s (volta07) and 12 900 s (pinky26) (g = 7.1 from (2,0,1) → (3,0,1)).",
]
for t in TOP: w(t)
w("")
w("## Open points for the coordinator")
w("- Run `cluster-pending.sh` once before copying any file. My duplicate check is offline: rows files plus brain-data at 518dc6761.")
w("- 51 exports are re-runs of exports already done elsewhere (sidecar note `re-export`). An import of the brain-data copies would remove them, but that is a manual step.")
w("- Held: OmniPaxos ⊑ S_omni (3,0,1) (see Phase B).")
w("- My generator scripts are in /tmp/su1110 (an.py, gen.py, md.py). They are not committed because the order allowed only these two output locations. To make the rows reproducible under R4, say whether they should go into `daavar/scripts/`.")
open(G+'plan/eval2/SCALEUP-2026-11-10.md','w').write('\n'.join(O)+'\n')
print(len(O))
