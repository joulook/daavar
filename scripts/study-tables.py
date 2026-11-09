#!/usr/bin/env python3
"""One generator for every case-study table, in the shape of REALSW-RESULTS (owner 2026-11-06: one structure for all tables):
instance = implementation side vs specification side (size), G_impl, G_spec, [G_x], the online block (M, T_exp, T_run, T_oth, T) and
the offline block (M, T_exp, T_join, T); pairs where both kinds exist, then the kind-only leftovers, then the runs without a verdict.
usage: study-tables.py <repo-root> <study>...   (studies: omnipaxos pineapple cassandra zookeeper swiftpaxos chainpaxos craq bk crdb)
-> plan/twin-tables/<STUDY>-STUDY.tex"""
import csv, sys, re, glob
R = sys.argv[1]
def esc(s): return s.replace('_', '\\_').replace('%', '\\%')
def fnum(x):   # seconds: 1 decimal from 10 s, 2 below, 3 below 0.1 s; a run below the timer's 10 ms resolution prints as <0.01
    try: v = float(x)
    except Exception: return '--'
    if v == 0: return '$<$0.01'
    from decimal import Decimal, ROUND_HALF_UP   # half-up on the value's text: 23.95 prints 24.0, not 23.9 (audit 1108-1703)
    d = lambda nd: str(Decimal(str(x).strip()).quantize(Decimal(1).scaleb(-nd), rounding=ROUND_HALF_UP))
    if v >= 10: return d(1)
    if v >= 0.1: return d(2)
    return d(3)
def mib(x):
    try: return str(round(float(x)))
    except Exception: return '--'
def gi(x):
    try: return int(float(x))
    except Exception: return -1
def status(r):
    v = r['verdict']; return r'\faClock' if v == 't/o' else ('oom' if v == 'oom' else ('--' if v == '' else None))
def off_cells(r):  # M = max of the three peaks; T_exp = the longer export; T_join; T = sum; a missing component prints as --
    st = status(r)
    M = mib(r['peak MB'])   # tables.py composes it: the two exports' peaks summed (side by side), or the join's peak if larger
    try:
        if (r.get('spec export s') or '') == '' or (r.get('impl export s') or '') == '' or (r.get('join s') or '') == '': te = tj = tot = None
        else: te = max(float(r['spec export s']), float(r['impl export s'])); tj = float(r['join s']); tot = te + tj
    except Exception: te = tj = tot = None
    if te is None and not st: return [M, '--', '--', '--']
    if st: return [M, fnum(te) if te is not None else '--', st, st]
    return [M, fnum(te), fnum(tj), fnum(tot)]
def on_cells(r):  # in-process: the two phases and the rest (T_oth) of one run; cross-language online: the spec export + the online run
    st = status(r); M = mib(r['peak MB'])
    try:
        if float(r.get('spec phase s') or 0) > 0 and float(r.get('impl phase s') or 0) > 0:
            te = float(r['spec phase s']); tr = float(r['impl phase s']); tot = float(r['wall s']); oth = fnum(tot - te - tr) if tot - te - tr >= 0.005 else '$<$0.01'
        elif (r.get('spec export s') or '') != '':
            te = float(r['spec export s']); tr = float(r.get('run s') or r['wall s']); tot = float(r['wall s']); oth = '--'   # tables.py: wall = export + run
        else: te = tr = tot = oth = None
    except Exception: te = tr = tot = oth = None
    if te is None and not st: return [M, '--', '--', '--', fnum(r['wall s'])]
    if st: return [M, fnum(te) if te is not None else '--', st, '--', st]
    return [M, fnum(te), fnum(tr), oth, fnum(tot)]
# ---- the studies: names of the sides from a run id ---------------------------------------------------------------------
def parse(i):
    p = i.split('.'); pre = p[0]; d = p[1]; rest = '.'.join(p[2:]); m = re.search(r'\.vs-([a-z0-9-]+)', rest); vs = m.group(1) if m else None
    return pre, d, rest, vs, ('spec' if rest.startswith('spec_') else 'impl')
def paper_code_study(prefix, SL, SD, extra_specs):   # pineapple / swiftpaxos / chainpaxos: paper and code candidates in both languages
    def cand(d):
        m = re.match(prefix + r'-(paper|code)(-rust)?(-sol)?$', d)
        return d if not m else f"{m.group(1)} ({'sol' if m.group(3) else 'Opus'}, {'Rust' if m.group(2) else 'Java'})"
    def rust_twin(d): return d.replace('-sol', '') + '-rust' + ('-sol' if d.endswith('-sol') else '')
    def spec_name(x): return {'hand': SL, 'derived': SD, 'lin': SL, **extra_specs}.get(x) or cand(prefix + '-' + x)
    def roles(i):
        pre, d, rest, vs, side = parse(i)
        if pre == 'C3J' and d == 'pineapple-lin' and vs and vs in extra_specs: return (SL, SD, 'offline')
        if not d.startswith(prefix): return None
        if pre == 'C3J' and d == prefix + '-derived' and side == 'spec': return (SD, spec_name(vs), 'offline')
        if pre == 'C3J' and d == prefix + '-lin' and side == 'spec': return (SL, spec_name(vs), 'offline')
        if pre in ('C3J', 'XJ1') and vs: return (cand(d), spec_name(vs), 'offline')
        if pre == 'XC1': return (cand(rust_twin(d)), cand(d), 'offline')
        if pre == 'XC2': return (cand(d), cand(rust_twin(d)), 'offline')
        return None
    return roles, (lambda r: r['program'].startswith(prefix) or any(w in r['run id'] for w in extra_specs))   # the hand spec vs this study's derived spec runs under the hand spec's program name
def translit_study(prefix, port, SL, SD, hand_dir, derived_dir, extra):   # cassandra / zookeeper / craq / bk / crdb: a transliteration and ports
    def cand(d):
        if d == port: return 'Java transliteration (ref.)'
        m = re.match(re.escape(port) + r'(-rust)?(-sol)?$', d)
        if not m: return extra.get(d, d)
        return f"port ({'sol' if m.group(2) else 'Opus'}, {'Rust' if m.group(1) else 'Java'})"
    def spec_name(x): return {'hand': SL, 'derived': SD, 'rust': cand(port + '-rust'), 'rust-sol': cand(port + '-rust-sol'), **{k: SD for k in extra.get('_derived_words', [])}, **{k: SL for k in extra.get('_hand_words', [])}}.get(x) or x
    def roles(i):
        pre, d, rest, vs, side = parse(i)
        if pre == 'C3J' and d == 'omnipaxos' and vs == 'zkderived': return (SL, SD, 'offline')
        if not d.startswith(prefix): return None
        if pre == 'C3J' and d == hand_dir and side == 'spec': return (SL, spec_name(vs), 'offline')
        if pre == 'C3J' and d == derived_dir and side == 'spec': return (SD, spec_name(vs), 'offline')
        if pre in ('C3J', 'XJ1') and vs: return (cand(d), spec_name(vs), 'offline')
        if pre == 'XC1': return (spec_name(vs) if vs else cand(port + '-rust' + ('-sol' if d.endswith('-sol') else '')), cand(d), 'offline')
        if pre == 'XC2': return (cand(d), spec_name(vs) if vs else cand(port + '-rust' + ('-sol' if d.endswith('-sol') else '')), 'offline')
        return None
    return roles, (lambda r: (r['program'].startswith(prefix) and not any(r['program'].startswith(x) for x in extra.get('_exclude', []))) or any(w in r['run id'] for w in extra.get('_derived_words', [])))   # _exclude: a longer prefix that is its own study (cassandra-rr; audit 1107-1737)
ONAME = {'omnipaxos-paper': 'paper (Opus)', 'omnipaxos-paper-sol': 'paper (sol)', 'omnipaxos-rust': 'rust-src (Opus)', 'omnipaxos-rust-sol': 'rust-src (sol)', 'omnipaxos-derived-impl': 'S$_{\\mathrm{paper}}$-impl (Opus)', 'omnipaxos-derived-sol': 'S$_{\\mathrm{paper}}$-impl (sol)', 'omnipaxos-spec': 'S$_{\\mathrm{raft}}$-impl (Opus)', 'omnipaxos-spec-sol': 'S$_{\\mathrm{raft}}$-impl (sol)'}
def omnipaxos_roles(i):
    SR, SP = 'S$_{\\mathrm{raft}}$', 'S$_{\\mathrm{paper}}$'
    def spec_name(x): return {'hand': SR, 'derived': SP, 'paper': ONAME['omnipaxos-paper'], 'spec': ONAME['omnipaxos-spec'], 'paper-sol': ONAME['omnipaxos-paper-sol'], 'spec-sol': ONAME['omnipaxos-spec-sol']}.get(x, x)
    pre, d, rest, vs, side = parse(i)
    if pre == 'C3J' and d == 'omnipaxos' and side == 'spec': return (SR, spec_name(vs), 'offline')
    if pre == 'C3J' and d == 'omnipaxos-derived': return (SP, spec_name(vs), 'offline')
    if pre == 'C3J' and d == 'omnipaxos': return ('Rust OmniPaxos', spec_name(vs) if vs else SR, 'offline')
    if pre == 'C1' and d == 'omnipaxos': return ('Rust OmniPaxos', SR, 'online')
    if pre == 'XJ1': return (ONAME.get(d, d), spec_name(vs), 'offline')
    if pre == 'XC1' and vs: return (ONAME.get(d, d), spec_name(vs), 'offline')
    if pre == 'XC1': return ('Rust OmniPaxos', ONAME.get(d, d), 'offline')
    if pre == 'XC2': return (ONAME.get(d, d), 'Rust OmniPaxos', 'offline')
    if pre == 'XCON2': return ('Rust OmniPaxos', ONAME.get(d, d), 'online')
    return None
SL, SD = 'S$_{\\mathrm{lin}}$', 'S$_{\\mathrm{der}}$'
STUDIES = {
 'omnipaxos': dict(roles=omnipaxos_roles, sel=lambda r: r['program'].startswith('omnipaxos'), new='2026-11-03T23:30', size='(e, t, cand)',
   title='The OmniPaxos study: two specifications, one Rust library, eight context-free Java candidates',
   text=r"S$_{\mathrm{raft}}$ = the client-visible content of Ongaro's TLA$^+$ specification of Raft (one decided log, deliveries in log order, a proposal appended or lost); S$_{\mathrm{paper}}$ = the specification derived from the Omni-Paxos paper by a context-free agent (version 3: decides each proposal as it arrives). Rust OmniPaxos = the unmodified library under a harness. The Java candidates were written by context-free agents from the paper, from the Rust library's source, from S$_{\mathrm{paper}}$ and from S$_{\mathrm{raft}}$, by Opus and by Codex sol. A row is one check: the implementation side against the specification side (which may itself be a specification or another implementation), at a size (e, t, cand)."),
 'pineapple': dict(*[()], **dict(zip(('roles', 'sel'), paper_code_study('pineapple', SL, SD, {}))), new='2026-11-04T18:00', size='q',
   title='The Pineapple study (NSDI 2025): two specifications, eight context-free candidates from the paper and from the public Go prototype',
   text=r"S$_{\mathrm{lin}}$ = the hand-written specification of Pineapple's single-key interface (one object applying Write, Read and the one-shot Rmw $f(v)=v+10$ in arrival order and answering the client named in the request); S$_{\mathrm{der}}$ = the specification derived from the paper by a context-free agent (Opus), reviewed by Astra (faithful). The candidates were written by context-free agents in Rust (TraceForge) and in Java (JMC), from the paper (sections 2.2, 3, Appendix A) and from the public Go prototype's sources (pineapple.go, pineappleproto.go, genericsmr.go), by Opus and by Codex sol; none saw a specification. A row is one check at $q$ operations per client (two clients, three nodes, stable leader, fifo)."),
 'swiftpaxos': dict(**dict(zip(('roles', 'sel'), paper_code_study('swiftpaxos', SL, SD, {'swiftderived': SD}))), new='2026-11-04T23:00', size='q',
   title='The SwiftPaxos study (NSDI 2024): two specifications, eight context-free candidates from the paper and from the public Go prototype',
   text=r"S$_{\mathrm{lin}}$ = the hand-written linearizable register specification reused from the Pineapple study (one object applying Write, Read and the one-shot Rmw $f(v)=v+10$ in arrival order); S$_{\mathrm{der}}$ = the specification derived from the SwiftPaxos paper by a context-free agent (Opus), reviewed by Astra (faithful). The candidates were written by context-free agents in Rust (TraceForge) and in Java (JMC), from the paper (sections 2--4, Appendix A) and from the public Go prototype's sources (swift/, replica/, client/, state/), by Opus and by Codex sol; none saw a specification. The paper's client code runs in invisible client libraries, so the clients' interface is Pineapple's. A row is one check at $q$ commands per client (two clients, three replicas, ballot 0 with leader n0, fast quorum $\{n_0,n_1\}$, fifo)."),
 'chainpaxos': dict(**dict(zip(('roles', 'sel'), paper_code_study('chainpaxos', SL, SD, {'chainderived-mixed': SD, 'chainderived-local': SD}))), new='2026-11-05T00:30', size='q, variant',
   title='The ChainPaxos study (USENIX ATC 2022): two specifications, six context-free candidates from the paper and from the Java implementation',
   text=r"S$_{\mathrm{lin}}$ = the hand-written linearizable register specification reused from the Pineapple study; S$_{\mathrm{der}}$ = the specification derived from the ChainPaxos paper by a context-free agent (Opus), reviewed by Astra (too loose for the local-read variant under the interface's bounded no-op rule). The candidates were written by context-free agents in Rust (TraceForge) and in Java (JMC), from the paper (sections 3--4) and from the public Java implementation (chainpaxos, frontend, app packages), by Opus and by Codex sol; none saw a specification. A row is one check at $q$ commands per client (two clients at the head and the tail of a fixed chain n0--n1--n2, n0 the leader, no failure, fifo), variant mixed (reads ordered through the chain) or local (the paper's local reads, the no-op timer as a bounded nondeterministic choice)."),
 'cassandra': dict(**dict(zip(('roles', 'sel'), translit_study('cassandra', 'cassandra-paxos-v1', 'S$_{\\mathrm{cas}}$', SD, 'cassandra-cas', 'cassandra-derived', {'_hand_words': ['cas'], '_exclude': ['cassandra-rr']}))), new='2026-11-04T20:00', size='q, variant',
   title='The Cassandra study: lightweight transactions (Paxos v1) of Apache Cassandra 4.1, two specifications, a transliteration and three context-free ports',
   text=r"S$_{\mathrm{cas}}$ = the hand-written specification of a linearizable compare-and-set register with unknown outcomes (a request may be answered Failed, and a failed Cas may still take effect later); S$_{\mathrm{der}}$ = the specification derived by a context-free agent (Opus) from Cassandra's own design document (service/paxos/Paxos.md), reviewed by Astra (it admits a decided Cas answered as not applied). The Java transliteration keeps Cassandra's classes and methods on JMC's API (the reference, not scored); the ports were written by context-free agents from the same sources in Rust (TraceForge) and Java (JMC), by Opus and by Codex sol. A row is one check at $q$ operations per client (two clients, three replicas, no failure, fifo), variant v1 or v1nolin."),
 'cassandra-rr': dict(**dict(zip(('roles', 'sel'), translit_study('cassandra-rr', 'cassandra-rr', 'S$_{\\mathrm{rr}}$', SD, 'cassandra-rr', 'cassandra-rr-derived', {}))), new='2026-11-05T20:00', size='q, strategy',
   title='The Cassandra read-repair study: quorum reads with read repair (blocking or none) of Apache Cassandra 4.1, two specifications, a transliteration and three context-free ports',
   text=r"S$_{\mathrm{rr}}$ = the hand-written specification of the read path (one object; writes applied on arrival, acknowledgements deferrable; under blocking read repair the per-cell floors rise with acknowledgements and returned values, under none with acknowledgements only); S$_{\mathrm{der}}$ = the specification derived by a context-free agent (Opus) from Cassandra's read-repair documentation, reviewed by Astra. The transliteration (Opus, Java) follows Cassandra's code under the logical-task contract of the study; the ports are context-free (Opus in Rust, Codex in Rust and in Java). Sizes: q = requests per client, then the read-repair strategy."),
 'zookeeper': dict(**dict(zip(('roles', 'sel'), translit_study('zookeeper', 'zookeeper-zab', 'S$_{\\mathrm{raft}}$', 'S$_{\\mathrm{zab}}$', '-', 'zookeeper-derived', {'_derived_words': ['zkderived'], 'zookeeper-zab-opus': 'port (Opus, Java)'}))), new='2026-11-04T20:30', size='e, late',
   title='The ZooKeeper study: atomic broadcast (Zab) of Apache ZooKeeper 3.9, two specifications, a transliteration and three context-free ports',
   text=r"S$_{\mathrm{raft}}$ = the client-visible log specification used for Raft and OmniPaxos (one decided log, deliveries in log order, a proposal appended or lost), reused unchanged; S$_{\mathrm{zab}}$ = the specification derived by a context-free agent (Opus) from the ZooKeeper project's TLA$^+$ specification Zab.tla (a sequencer: no proposal lost under a fixed leader), reviewed by Astra. The Java transliteration keeps ZooKeeper's classes and methods on JMC's API (the reference, not scored); the ports were written by context-free agents from the same sources in Rust (TraceForge) and Java (JMC), by Opus and by Codex sol. A row is one check at $e$ proposals per client (two clients, three servers, n0 the fixed leader, no failure, fifo) with late = 0 or 1 (n2 joins after the first delivery)."),
 'craq': dict(**dict(zip(('roles', 'sel'), translit_study('craq', 'craq', SL, SD, 'craq-lin', 'craq-derived', {}))), new='2026-11-06T13:00', size='q',
   title='The 3FS study (DeepSeek 3FS, chain replication of one chunk, CRAQ): two specifications, three context-free candidates and the transliteration',
   text=r"S$_{\mathrm{lin}}$ = the hand-written atomic-register specification; S$_{\mathrm{der}}$ = the specification derived from 3FS's design notes by a context-free agent (Opus), reviewed by Astra (faithful). The candidates were written by context-free agents from the 3FS sources (the storage client, the storage service's chain update and read paths, the chunk store), in Rust (Opus, Codex sol) and in Java (Codex sol); the transliteration (Opus, Java) is the unscored reference under the logical-task and shared-memory contracts. A row is one check at $q$ operations per client (two clients, a chain of three targets, no failure, fifo, the not-committed read retry cut per target)."),
 'bk': dict(**dict(zip(('roles', 'sel'), translit_study('bk', 'bk', 'S$_{\\mathrm{bk}}$', SD, 'bk-ledger', 'bk-derived', {'bk-413': 'Java transliteration 4.13 (ref.)'}))), new='2026-11-06T00:00', size='e',
   title='The BookKeeper study (Apache BookKeeper, ledger replication with fencing): two specifications, three context-free candidates of main, the transliterations of main and of release 4.13',
   text=r"S$_{\mathrm{bk}}$ = the hand-written specification from the protocol's TLA$^+$ model (acknowledged entries form a prefix; the recovery closes at or above every acknowledged entry; nothing is acknowledged after the close); S$_{\mathrm{der}}$ = the specification derived from the model's README and master TLA$^+$ by a context-free agent (Opus), reviewed by Astra (faithful). The candidates were written by context-free agents from the main sources at 7b66b5d, in Rust (Opus, Codex sol) and in Java (Codex sol); the transliterations (Opus, Java) of main and of release-4.13.0 are unscored references under the logical-task contract (4.13: the recovery reads do not fence, the model's first documented finding). A row is one check at $e$ entries of the writer (one writer, one recovery client, three bookies, write quorum 3, ack quorum 2, no failure, fifo)."),
 'crdb': dict(**dict(zip(('roles', 'sel'), translit_study('crdb-pc', 'crdb-pc', 'S$_{\\mathrm{pc}}$', SD, 'crdb-pc', 'crdb-pc-derived', {}))), new='2026-11-06T00:00', size='(n, p, m, a)',
   title='The CockroachDB study (parallel commits with transaction recovery, d30c905): two specifications, three context-free candidates and the transliteration',
   text=r"S$_{\mathrm{pc}}$ = the hand-written specification from the authors' TLA$^+$ model (one fate for the committer and every preventer; an acknowledged commit is never aborted; no retry in this failure-free setting); S$_{\mathrm{der}}$ = the specification derived from the parallel-commits RFC by a context-free agent (Opus), reviewed by Astra (faithful, round 2). The candidates were written by context-free agents from the Go sources of the cut, in Rust (Opus, Codex sol) and in Java (Codex sol); the transliteration (Opus, Java) is the unscored reference under the logical-task contract. A row is one check at $(n,p,m,a)$ = keys, pipelined writes, preventers, epochs (no failure, logical timestamps, fifo)."),
 'scalardb': dict(**dict(zip(('roles', 'sel'), translit_study('scalardb-cc', 'scalardb-cc', 'S$_{\\mathrm{cc}}$', SD, 'scalardb-cc', 'scalardb-cc-derived', {}))), new='2026-11-06T01:00', size='(pattern, k, iso)',
   title='The ScalarDB study (Consensus Commit of scalar-labs/scalardb at 416cdc2): two specifications, three context-free candidates and the transliteration',
   text=r"S$_{\mathrm{cc}}$ = the hand-written specification from the authors' TLA$^+$ model (the atomic commitment of one transaction) and the isolation guarantees the code and the documentation state (committed reads only, all-or-nothing visibility, first-updater-wins, serializable validation, the read-abort rule); S$_{\mathrm{der}}$ = the specification derived from the public Consensus Commit documentation by a context-free agent (Opus), reviewed by Astra. The candidates were written by context-free agents from the Java sources of the cut (the transaction object and its workspace, reads and lazy recovery, the commit phases, the coordinator-state accessor, the mutation composers), in Rust (Opus, Codex sol) and in Java (Codex sol); the transliteration (Opus, Java) is the unscored reference under the logical-task contract. A row is one check at (pattern, k, iso) = the clients' read-write pattern (lost, skew, both, mixed, mixedro), the number of keys and the isolation level (no failure, the writer's expiry as a free choice, fifo)."),
}
COMMON = r" Every row is one instance (implementation side, specification side, size), per communication model; one run each; a time or memory limit is shown only from the 48 h tier. The superscript is the communication model (B = bag/asyn, F = fifo/p2p). $G_{\mathit{cand}}$, $G_{\mathit{ref}}$ = the graphs of the checked side and of the side it is checked against; $G_x$ = the checked side's graphs that no graph of the other side covers (violating tables). Online, in-process: one run explores the specification's graphs and indexes their summaries, then explores the implementation's graphs and queries the index: M = its peak memory (MiB), T$_{\mathit{exp}}$ = its specification phase, T$_{\mathit{run}}$ = its implementation phase (exploration and queries), T$_{\mathit{oth}}$ = the rest of the run outside the two phase timers (process start, the index, the report after a violation, shutdown), T = its wall time = T$_{\mathit{exp}}$ + T$_{\mathit{run}}$ + T$_{\mathit{oth}}$. Online, cross-language: M = the larger of the export's peak and the run's, T$_{\mathit{exp}}$ = the export run of the specification side, T$_{\mathit{run}}$ = the online run that loads it, T$_{\mathit{oth}}$ = --, T = their sum. Offline: each side's export run explores its graphs and writes the sorted set of its distinct summaries; the join compares the two sets: M = the sum of the two exports' peaks (the two jobs run side by side and are resident at once), or the join's peak if larger, T$_{\mathit{exp}}$ = the longer export run, T$_{\mathit{join}}$ = the join, T = their sum. Times in seconds, 1 decimal from 10 s, 2 decimals below, 3 below 0.1 s; a run below the 10 ms resolution of the timer prints as $<$0.01; a component without a measurement prints as --, never as 0. \faClock\ = time limit reached, oom = memory limit reached. \textcolor{blue}{Blue} = a run of this study's campaign; \textcolor{orange}{orange} = an earlier run that shared its NUMA node with another run for more than 30 s, whose times are not to be trusted; \textcolor{red}{red} = an earlier run that had its node to itself."
def viol_of(r): return r['verdict'] == 'violation'   # the actual verdict (Opus audit 11-06: classifying by the expected verdict hid real violations)
HDR = lambda viol: (r'\multicolumn{' + str(4 if viol else 3) + r'}{c}{} & \multicolumn{5}{c}{online} & \multicolumn{4}{c}{offline} \\ \cmidrule(lr){' + str(5 if viol else 4) + '-' + str(9 if viol else 8) + r'}\cmidrule(lr){' + str(10 if viol else 9) + '-' + str(13 if viol else 12) + r'}' + '\n' + r'instance & $G_{\mathit{cand}}$ & $G_{\mathit{ref}}$' + (r' & $G_x$' if viol else '') + r' & M & T$_{\mathit{exp}}$ & T$_{\mathit{run}}$ & T$_{\mathit{oth}}$ & T & M & T$_{\mathit{exp}}$ & T$_{\mathit{join}}$ & T \\')
for study in sys.argv[2:]:
    S = STUDIES[study]; roles = S['roles']; NEW = S['new']
    def paint(r, cells): c = 'blue' if (r.get('started') or '') >= NEW else ('orange' if r.get('contended') == 'yes' else 'red'); return [r'\textcolor{' + c + '}{' + x + '}' for x in cells]
    def label(r):   # the size, with the variant when the run id carries one beyond the size (chainpaxos mixed/local, cassandra v1nolin)
        v = r.get('variant') or ''; return r['size'] + ((' ' + v) if v and v not in ('impl', 'spec', 'correct', 'v1') and v not in r['size'] else '')
    def inst(k): m = {'bag': 'B', 'fifo': 'F', 'p2p': 'F', 'asyn': 'B', 'causal': 'C'}.get(k[3], ''); return esc(k[0]) + ' vs ' + esc(k[1]) + (r'$^{\mathrm{' + m + '}}$' if m else '') + '(' + esc(k[2]) + ')'
    best = {}; nov = []
    for f in glob.glob(R + '/PLDI/tex/tables/campaign-*.csv'):
        for r in csv.DictReader(open(f)):
            if not S['sel'](r): continue
            ro = roles(r['run id'])
            if not ro: continue
            if r['verdict'] in ('t/o', 'oom') and (r.get('timeout s') or '') != '172800': continue
            kind = 'online' if ro[2] in ('online', 'in-process') else 'offline'
            if r['verdict'] not in ('conforms', 'violation', 't/o', 'oom'): nov.append((kind, r)); continue
            k = (ro[0], ro[1], label(r), r['model']); cur = best.get((k, kind))
            if cur is None or (r.get('started') or '') > (cur.get('started') or ''): best[(k, kind)] = r
    L = [r'\documentclass[9pt]{article}', r'\usepackage[a4paper,landscape,margin=12mm]{geometry}', r'\usepackage{booktabs,longtable,amsmath,xcolor,fontawesome5}', r'\usepackage[T1]{fontenc}', r'\setlength{\tabcolsep}{3.5pt}', r'\begin{document}', r'\section*{' + S['title'] + '}', S['text'] + COMMON, r'\medskip']
    keys = {k for k, _ in best}; both = {k for k in keys if (k, 'online') in best and (k, 'offline') in best}
    def tot(r):
        try: return float(r['wall s'])
        except Exception: return -1
    for title, sel in (('both checks', sorted(both)), ('offline only', sorted(k for k in keys if (k, 'offline') in best and k not in both)), ('online only', sorted(k for k in keys if (k, 'online') in best and k not in both))):
        for viol in (False, True):
            rows = []
            for k in sel:
                ro = best.get((k, 'offline')); rn = best.get((k, 'online')); r0 = ro or rn
                if viol_of(r0) != viol: continue
                rows.append((k, ro, rn))
            if not rows: continue
            rows.sort(key=lambda x: (-tot(x[1] or x[2]), x[0]))
            L += [r'\begin{longtable}{@{}l' + ('rrr' if viol else 'rr') + r'rrrrrrrrr@{}}', r'\caption{' + ('Violating' if viol else 'Conforming') + ' pairs, ' + title + ' (' + str(len(rows)) + r' instances).}\\', r'\toprule', HDR(viol), r'\midrule', r'\endfirsthead', r'\toprule', HDR(viol), r'\midrule', r'\endhead', r'\bottomrule', r'\endfoot']
            for k, ro, rn in rows:
                r0 = ro or rn
                cells = [inst(k), r0['impl graphs'] or '--', r0['spec graphs'] or '--'] + ([(ro or {}).get('uncovered', '') or (rn or {}).get('uncovered', '') or '--'] if viol else [])
                on = paint(rn, on_cells(rn)) if rn else ['--'] * 5; off = paint(ro, off_cells(ro)) if ro else ['--'] * 4
                L.append(' & '.join(paint(r0, cells) + on + off) + r' \\')
            L += [r'\end{longtable}']
    if nov:
        L += [r'\subsection*{Runs that ended without a verdict}', r'These runs are in no table above: the tool stopped by itself without a verdict (its last line is quoted), or the record carries neither a verdict nor a limit.', r'\medskip', r'\begin{longtable}{@{}llrrp{95mm}@{}}', r'\caption{Runs without a verdict (' + str(len(nov)) + r').}\\', r'\toprule', r'instance & kind & T$_{\mathit{run}}$ & M & outcome \\', r'\midrule', r'\endfirsthead', r'\toprule', r'instance & kind & T$_{\mathit{run}}$ & M & outcome \\', r'\midrule', r'\endhead', r'\bottomrule', r'\endfoot']
        for kind, r in sorted(nov, key=lambda x: x[1]['run id']):
            ro = roles(r['run id']); k = (ro[0], ro[1], label(r), r['model'])
            why = (r.get('stop reason') or 'no stop reason') + ((': ' + esc(r['failure'])) if r.get('failure') else '') + r' \quad {\footnotesize\texttt{' + esc(r['run id']) + '}}'
            L.append(' & '.join(paint(r, [inst(k), kind, fnum(r['wall s']), mib(r['peak MB'])]) + [why]) + r' \\')
        L += [r'\end{longtable}']
    L.append(r'\end{document}')
    open(R + '/plan/twin-tables/' + study.upper() + '-STUDY.tex', 'w').write('\n'.join(L) + '\n'); print(study, 'study written:', len(best), 'checks,', len(nov), 'without a verdict')
