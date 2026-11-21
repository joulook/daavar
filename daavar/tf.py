"""The TraceForge adapter: the Rust program's command lines (§3.3 steps 3, 5), conf-join through
`scripts/witness-join.sh` (step 6), and their outcomes (§4.1, then the parsed output for §4.2).

Only this module and `jmc` know a tool's command line.
"""

from __future__ import annotations

from pathlib import Path

from . import outcome, parse
from .outcome import Outcome


# The enumeration each stop reason leaves (SPEC §2; online.rs:783-787).
EXPORT_STOPS = {"exhausted": ("complete",)}
ONLINE_STOPS = {"exhausted": ("complete",), "budget": ("bounded",), "first-miss": ("aborted",),
                "threshold": ("aborted",)}


def program_args(args_text: str) -> list:
    """ARGS after `--`: the one comma-joined argument, or nothing (no empty argument)."""
    return ["--", args_text] if args_text else ["--"]


def export_argv(binary, side: str, out, graphs, model: str, program: str, args_text: str, threads: int = 1) -> list:
    """`<bin> export <side> --out E --graphs E.graphs.jsonl --model M --program <name> [--threads N
    --graphs-limit 0] -- ARGS` (TF/program.rs:617,646; N > 1 stores no graphs, :282). `--size` is not
    passed: it defaults to the joined ARGS (:680)."""
    a = [str(binary), "export", side, "--out", str(out), "--graphs", str(graphs), "--model", model,
         "--program", program]
    if threads > 1:
        a += ["--threads", str(threads), "--graphs-limit", "0"]
    return a + program_args(args_text)


def online_argv(binary, spec, out, graphs, model: str, program: str, args_text: str,
                stop_at_first: bool = False) -> list:
    """`<bin> online --spec S --out E --graphs G --model M --program <name> [--stop-at-first] -- ARGS`
    (TF/program.rs:236; `--stop-at-first` is refused above one thread, :851, so no `--threads`)."""
    a = [str(binary), "online", "--spec", str(spec), "--out", str(out), "--graphs", str(graphs), "--model", model,
         "--program", program]
    if stop_at_first:
        a.append("--stop-at-first")
    return a + program_args(args_text)


def join_argv(script, ref, checked, outdir, decide_budget: int, witness_limit, witness_nearest: int,
              merge: bool = False) -> list:
    """`scripts/witness-join.sh <ref> <checked> joins/<checked>-in-<ref> [--merge] --decide-budget B
    --witness-limit L --witness-nearest K` (the script adds --decide; witness-join.sh:40,59,106)."""
    a = [str(script), str(ref), str(checked), str(outdir)]
    if merge:
        a.append("--merge")
    return a + ["--decide-budget", str(decide_budget), "--witness-limit", str(witness_limit),
                "--witness-nearest", str(witness_nearest)]


def conf_join_build_argv() -> list:
    """§3.2: conf-join is built from the TraceForge workspace."""
    # --locked (review S1-S6 F7): cargo never rewrites the tool checkout's Cargo.lock.
    return ["cargo", "build", "--release", "--locked", "-p", "traceforge", "--bin", "conf-join"]


def _unparsable(e: Exception, exit_code: int) -> Outcome:
    return Outcome(outcome.REFUSED, f"exit {exit_code}, but the output is not what the contract promises: {e}",
                   "contributing", "unparsable", exit_code)


def export_outcome(exit_code: int, timed_out: bool, stdout: str, stderr: str, out_path) -> Outcome:
    """A Rust `export`: §4.1, then the export file (TraceForge writes only `complete`)."""
    o = outcome.classify("tf", "export", exit_code, timed_out, stdout, stderr)
    if not o.ok:
        return o
    try:
        ex = parse.export_file(out_path)
        if ex.header.get("tool") != "traceforge":
            raise parse.ParseError(f"{out_path}: tool {ex.header.get('tool')!r}, not traceforge")
        if not ex.complete:
            raise parse.ParseError(f"{out_path}: TraceForge export wrote {ex.enumeration!r}, not complete")
        line = parse.tf_exported_line(stdout)
        if line is None:
            raise parse.ParseError("no `exported:` line")
        if line["lines"] != ex.lines:
            raise parse.ParseError(f"`exported:` says {line['lines']} lines, the file holds {ex.lines}")
        _, meas, _ = parse.split_measurements(stdout)
        parse.check_counts(ex, meas, EXPORT_STOPS, "export")
        o.data = {"export": ex, "store": parse.graph_store_line(stdout), "measurements": meas, "exported": line,
                  "stopped_early": False}
    except parse.ParseError as e:
        return _unparsable(e, exit_code)
    return o


def online_outcome(exit_code: int, timed_out: bool, stdout: str, stderr: str, out_path) -> Outcome:
    """A Rust `online --out`: §4.1, then the verdict lines and the export (complete, bounded or aborted).
    A `--stop-at-first` stop leaves the export `aborted` (online.rs:783-786): `stopped_early`."""
    o = outcome.classify("tf", "online", exit_code, timed_out, stdout, stderr)
    if not o.ok:
        return o
    try:
        ol = parse.online_lines(stdout)
        if ol is None:
            raise parse.ParseError("no `verdict:` line")
        if ol["verdict"] == "refused":
            return Outcome(outcome.REFUSED, "online verdict refused (a runtime name refusal)", "roles",
                           "online-refused", exit_code)
        ex = parse.export_file(out_path)
        if ex.header.get("tool") != "traceforge":
            raise parse.ParseError(f"{out_path}: tool {ex.header.get('tool')!r}, not traceforge")
        _, meas, _ = parse.split_measurements(stdout)
        parse.check_counts(ex, meas, ONLINE_STOPS, "online")
        stopped = meas.get("stop_reason") == "first-miss"
        o.data = {"export": ex, "online": ol, "store": parse.graph_store_line(stdout), "measurements": meas,
                  "stopped_early": stopped}
    except parse.ParseError as e:
        return _unparsable(e, exit_code)
    return o


def join_outcome(exit_code: int, timed_out: bool, stdout: str, stderr: str, outdir, witness: bool = True) -> Outcome:
    """A witness-join.sh run (conf-join's exit code): §4.1, then join.out, graphs/index.tsv and summary.txt,
    every count checked (§3.3 step 6: anything missing or truncated is refused, never 0)."""
    d = Path(outdir)
    jerr = (d / "join.err").read_text(errors="replace") if (d / "join.err").exists() else ""
    o = outcome.classify("conf-join", "join", exit_code, timed_out, stdout, stderr + "\n" + jerr)
    if not o.ok:
        return o
    try:
        text = (d / "join.out").read_text(encoding="utf-8")
    except FileNotFoundError:
        return _unparsable(parse.ParseError(f"{d}/join.out is missing"), exit_code)
    try:
        rep = parse.conf_join(text, exit_code)
        if rep.decide is None:
            raise parse.ParseError("no decide counters (witness-join.sh adds --decide)")
        rows = parse.index_tsv(d / "graphs" / "index.tsv", rep) if witness else []
        summ = parse.summary_txt(d / "summary.txt")
        if summ["exit"] != str(exit_code) or summ["uncovered"] != str(rep.uncovered):
            raise parse.ParseError(f"summary.txt exit={summ['exit']} uncovered={summ['uncovered']} "
                                   f"disagree with exit {exit_code}, uncovered {rep.uncovered}")
        o.data = {"report": rep, "index": rows, "summary": summ}
    except parse.ParseError as e:
        return _unparsable(e, exit_code)
    return o
