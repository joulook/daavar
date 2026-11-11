#!/usr/bin/env python3
"""case-scaleup-1111 step 1: load brain-data records, every rows file under cluster/campaign/ and the live CASE rows (running and
pending, tmp/sweep-1111) of the 13 case studies of plan/eval2/case-scaleup-1111/BRIEF.md; pickle to $SCR/load.pkl."""
import csv, glob, os, pickle, re, sys
csv.field_size_limit(10**9)
G = '/local/home/mkhoshechin/Documents/github-jmc/'; SCR = '/local/home/mkhoshechin/tmp/case-scaleup-1111/'
SW = '/local/home/mkhoshechin/tmp/sweep-1111/'
DIRRE = re.compile(r'^(omnipaxos.*|raft-rs|cassandra.*|tikv.*|zookeeper-zab.*|zookeeper-derived|pineapple.*|scalardb.*|crdb.*|chainpaxos.*|'
                   r'bk|bk-413|bk-sol|bk-rust.*|bk-ledger|bk-derived|craq.*|swiftpaxos.*)$')
def indir(i):
    p = i.split('.')
    return len(p) > 2 and bool(DIRRE.match(p[1])) and 'zab-broadcast-remix' not in p[1]
recs = []
for f in sorted(glob.glob(G + 'brain-data/results/*.tsv')):
    for x in csv.DictReader(open(f), delimiter='\t'):
        if x.get('run_id') and indir(x['run_id']):
            x['_file'] = os.path.basename(f); recs.append(x)
rows = {}   # id -> list of (file, mem, timeout, cmd)
for f in sorted(glob.glob(G + 'daavar/cluster/campaign/**/*.rows', recursive=True)):
    for l in open(f):
        if l.startswith('#') or not l.strip(): continue
        t = l.rstrip('\n').split(' ', 3)
        if len(t) == 4 and indir(t[0]): rows.setdefault(t[0], []).append((os.path.relpath(f, G + 'daavar/cluster/campaign'), t[1], t[2], t[3]))
side = {}   # id -> sidecar attributes (first seen)
for f in sorted(glob.glob(G + 'daavar/cluster/campaign/**/*.rows.tsv', recursive=True)):
    try:
        for x in csv.DictReader(open(f), delimiter='\t'):
            if x.get('id') and indir(x['id']): side.setdefault(x['id'], x)
    except Exception as e: print('sidecar', f, e, file=sys.stderr)
live = {}
for l in open(SW + 'running-live.tsv'):
    t = l.rstrip('\n').split('\t')
    if len(t) > 3 and t[2] == 'CASE': live[t[3]] = ('running', t[1], t[5])
for l in open(SW + 'pending-dedup.tsv'):
    t = l.rstrip('\n').split('\t')
    if len(t) > 4 and t[2] == 'CASE': live[t[4]] = ('pending', t[1], t[3])
pickle.dump(dict(recs=recs, rows=rows, side=side, live=live), open(SCR + 'load.pkl', 'wb'))
print(len(recs), 'records', len(rows), 'row ids', len(side), 'sidecar ids', len(live), 'live CASE ids')
