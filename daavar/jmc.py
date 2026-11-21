"""The JMC adapter: MpLauncher's command lines (§3.3 steps 3, 5; §3.7) and their outcomes (§4.1).

Two CLI forms: `positional()` (the launcher at the pin, `L:336-384,450`) and `named()` (phase 2's named
options). Phase 2 has not landed at the pin (JMC c1bbf657 has no `--help`), so `named()` refuses until its
flags exist; `cli = "auto"` is resolved by `build`, which probes `--help` (exit 0 means named).
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


def named(*_a, **_k) -> list:
    raise DaavarError("the named MpLauncher CLI (phase 2) is not available at this JMC pin; set "
                      "[tools.jmc] cli = \"positional\" (or \"auto\")")


def argv(form: str, *a, **k) -> list:
    if form == "positional":
        return positional(*a, **k)
    if form == "named":
        return named(*a, **k)
    raise DaavarError(f"JMC CLI form {form!r} is not resolved (positional or named; `daavar build` resolves auto)")


def help_probe_argv(java: str, classpath: list, main: str) -> list:
    return [java, "-cp", ":".join(str(c) for c in classpath), main, "--help"]


def cli_from_probe(exit_code: int, stderr: str = "", timed_out: bool = False) -> str | None:
    """§3.7: `--help` exits 0 under the named CLI; the positional launcher refuses it with exit 2 and
    `cannot load and construct --help` (fixture jmc-help). Anything else decides nothing (None, not cached)."""
    if timed_out:
        return None
    if exit_code == 0:
        return "named"
    if exit_code == 2 and "cannot load and construct --help" in stderr:
        return "positional"
    return None


def export_outcome(exit_code: int, timed_out: bool, stdout: str, stderr: str, out_path, online: bool = False,
                   graphs=None) -> Outcome:
    """A JMC export (or, with `online`, an online check with an export): §4.1, then the export file and the
    verdict lines. JMC writes `complete`, `bounded` (also after a --stop-at-first stop) or `aborted` (its own
    timeout); a stop is read from the measurement block (`stop_reason: first-miss`)."""
    o = outcome.classify("jmc", "online" if online else "export", exit_code, timed_out, stdout, stderr)
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
