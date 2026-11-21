"""The JMC adapter: MpLauncher's command lines (§3.3 steps 3, 5; §3.7) and their outcomes (§4.1).

Two CLI forms: `positional()` (the launcher at c1bbf657, `L:336-384,450`; deprecated since JMC 0.3.0) and
`named()` (JMC 0.3.0, fc684f21: `MpLauncher export|online --program ...`, NamedCli.java). Both run the same code
and print the same lines (the named form adds a `model:` line); only the exit statuses differ (S15 fixtures,
DRIVER-IMPL-NOTES.md S15): positional 0 the run completed (whatever the verdict), 1 failed, 2 refused; named
0 conforms, 1 violation, 2 refused, 3 partial or incomplete. `named_to_positional()` maps a named run back so
that one §4.1 rule classifies both. `cli = "auto"` is resolved by `build` from the classpath (`has_named_cli`).
"""

from __future__ import annotations

from . import DaavarError, outcome, parse
from .outcome import Outcome

JVM_FLAGS = ("-ea", "-Xss4m")
# The enumeration each stop reason leaves under must (SPEC §2: bounded also after a --stop-at-first stop;
# complete when the miss was on the last graph, MpLauncher.java:622, 1095).
JMC_STOPS = {"exhausted": ("complete",), "budget": ("bounded",), "first-miss": ("bounded", "complete"),
             "timeout": ("aborted",)}


def java_prefix(java: str, xmx: str, classpath: list) -> list:
    return [java, *JVM_FLAGS, f"-Xmx{xmx}", "-cp", ":".join(str(c) for c in classpath)]


def positional(java: str, xmx: str, classpath: list, main: str, program_arg: str, seed: int, out, graphs,
               timeout_s: int, spec=None, stop_at_first: bool = False, report: str = "report") -> list:
    """`java -ea -Xss4m -Xmx<x> -cp <artifact>:<CP> <main> <Class:items> must 0 <seed> report E <timeoutS>
    --graphs E.graphs.jsonl [--spec S [--stop-at-first]]`, run in a fresh working directory (`report` is
    relative to it). Iterations 0 under must: unbounded, with the launcher's own timeout."""
    if stop_at_first and spec is None:
        raise DaavarError("--stop-at-first needs an online check (--spec)")
    a = java_prefix(java, xmx, classpath) + [main, program_arg, "must", "0", str(seed), report, str(out),
                                             str(timeout_s), "--graphs", str(graphs)]
    if spec is not None:
        a += ["--spec", str(spec)]
        if stop_at_first:
            a.append("--stop-at-first")
    return a


def named(java: str, xmx: str, classpath: list, main: str, program_arg: str, seed: int, out, graphs,
          timeout_s: int, spec=None, stop_at_first: bool = False, report: str = "report") -> list:
    """`java ... <main> export|online --program <Class:items> --strategy must --iterations 0 --seed <seed>
    --report-dir report --out E --timeout <timeoutS> --graphs E.graphs.jsonl [--spec S [--stop-at-first]]`:
    the positional command line's values under NamedCli's names (NamedCli.run() translates them back to the
    same positional arguments). No `--model`: the positional form has none, and the program's MpProgram
    declares it (the join's preflight compares it with the project's)."""
    if stop_at_first and spec is None:
        raise DaavarError("--stop-at-first needs an online check (--spec)")
    a = java_prefix(java, xmx, classpath) + [main, "online" if spec is not None else "export",
                                             "--program", program_arg, "--strategy", "must", "--iterations", "0",
                                             "--seed", str(seed), "--report-dir", report, "--out", str(out),
                                             "--timeout", str(timeout_s), "--graphs", str(graphs)]
    if spec is not None:
        a += ["--spec", str(spec)]
        if stop_at_first:
            a.append("--stop-at-first")
    return a


def argv(form: str, *a, **k) -> list:
    if form == "positional":
        return positional(*a, **k)
    if form == "named":
        return named(*a, **k)
    raise DaavarError(f"JMC CLI form {form!r} is not resolved (positional or named; `daavar build` resolves auto)")


# The named form's package (JMC 0.3.0): its launcher class on the classpath decides `cli = "auto"`.
NAMED_CLI_CLASS = "org/mpi_sws/jmc/mp/cli/MpLauncher.class"


def has_named_cli(classpath: list) -> bool:
    """Whether a classpath entry (a class directory or a jar) holds org.mpi_sws.jmc.mp.cli.MpLauncher."""
    import zipfile
    from pathlib import Path
    for e in classpath:
        p = Path(e)
        if p.is_dir() and (p / NAMED_CLI_CLASS).is_file():
            return True
        if p.is_file() and p.suffix == ".jar":
            try:
                with zipfile.ZipFile(p) as z:
                    if NAMED_CLI_CLASS in z.namelist():
                        return True
            except (OSError, zipfile.BadZipFile):
                continue
    return False


def named_to_positional(exit_code: int, stdout: str, online: bool) -> int:
    """The positional exit status of a named run (fixtures cases15/, S15): 0 and 2 stay; 1 with a `FAILED:`
    line is the positional 1 (a program failure, or a shape error the text rules refuse); 1 without one is an
    online `reports` verdict, which the positional form exits 0 with; 3 without `FAILED:` is a bounded or
    aborted export (or a `partial` verdict), positional 0, which §4.2 reads as incomplete. Anything else stays
    (classify refuses it), except 3 with `FAILED:`, which export_outcome classifies itself."""
    failed = any(l.startswith("FAILED:") for l in stdout.splitlines())
    if exit_code == 1:
        return 1 if failed else (0 if online and parse.online_lines(stdout) is not None else exit_code)
    if exit_code == 3 and not failed:
        return 0
    return exit_code


def export_outcome(exit_code: int, timed_out: bool, stdout: str, stderr: str, out_path, online: bool = False,
                   graphs=None, cli: str = "positional") -> Outcome:
    """A JMC export (or, with `online`, an online check with an export): §4.1, then the export file and the
    verdict lines. JMC writes `complete`, `bounded` (also after a --stop-at-first stop) or `aborted` (its own
    timeout); a stop is read from the measurement block (`stop_reason: first-miss`)."""
    if cli not in ("positional", "named"):
        raise DaavarError(f"JMC CLI form {cli!r} is not resolved (positional or named)")
    step = "online" if online else "export"
    if cli == "named" and not timed_out:
        o = _named_outcome(exit_code, stdout, stderr, step)
        if o is not None:
            return o
        try:
            pexit = named_to_positional(exit_code, stdout, online)
        except parse.ParseError as e:  # Astra S15 P2: the same refusal as the positional path's parse errors
            return Outcome(outcome.REFUSED, f"exit {exit_code}, but the output is not what the contract promises: "
                           f"{e}", "contributing", "unparsable", exit_code)
    else:
        pexit = exit_code
    o = outcome.classify("jmc", step, pexit, timed_out, stdout, stderr)
    o.exit = exit_code
    if o.kind == outcome.FAILURE and outcome.appended_output_failure(o.message):
        # MpLauncher writes the (aborted) export after a program failure; without it, the `cannot write`
        # segment is the launcher's own and the run is refused, never a violation (D4).
        # Both outputs are checked: the launcher also appends it when the graph store fails after the export
        # was written (MpLauncher.java:673).
        try:
            parse.export_file(out_path)
            parse.store_head(graphs if graphs is not None else f"{out_path}.graphs.jsonl")
        except parse.ParseError:
            return Outcome(outcome.REFUSED, o.message, "contributing", "output-failed", exit_code)
    if not o.ok:
        return o
    try:
        ex = parse.export_file(out_path)
        if ex.header.get("tool") != "jmc":
            raise parse.ParseError(f"{out_path}: tool {ex.header.get('tool')!r}, not jmc")
        line = parse.jmc_exported_line(stdout)
        if line is None:
            raise parse.ParseError("no `exported:` line")
        if line["lines"] != ex.lines or line["enumeration"] != ex.enumeration:
            raise parse.ParseError(f"`exported:` says {line['lines']} lines ({line['enumeration']}), the file "
                                   f"holds {ex.lines} ({ex.enumeration})")
        _, meas, _ = parse.split_measurements(stdout)
        parse.check_counts(ex, meas, JMC_STOPS, "jmc")
        ol = parse.online_lines(stdout) if online else None
        if online and ol is None:
            raise parse.ParseError("no `verdict:` line")
        stopped = online and (meas or {}).get("stop_reason") == "first-miss"
        o.data = {"export": ex, "online": ol, "store": parse.graph_store_line(stdout), "measurements": meas,
                  "exported": line, "stopped_early": stopped}
    except parse.ParseError as e:
        return Outcome(outcome.REFUSED, f"exit {exit_code}, but the output is not what the contract promises: {e}",
                       "contributing", "unparsable", exit_code)
    return o


def _named_outcome(exit_code: int, stdout: str, stderr: str, step: str) -> Outcome | None:
    """The named exits the positional text rules cannot decide alone (§4.1, S15): 2 is refused whatever the
    text (the deciding line and category from the text rules when they also refuse); 3 with `FAILED:` (an
    unfinished run) is incomplete; an exit 1 the text rules call a failure stays a failure. None: classify the
    mapped positional exit."""
    failed = parse.failed_line(stdout)
    if exit_code == 2:
        o = outcome.classify("jmc", step, 1 if failed is not None else 2, False, stdout, stderr)
        if o.kind == outcome.REFUSED:
            o.exit = 2
            return o
        return Outcome(outcome.REFUSED, o.message or "exit 2", o.category or "contributing", "named-refused", 2)
    if exit_code == 3 and failed is not None:
        return Outcome(outcome.INCOMPLETE, failed, "budgets", "named-incomplete", 3)
    return None
