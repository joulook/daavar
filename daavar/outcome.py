"""Process outcomes (DRIVER-DESIGN §4.1): one rule for both tools, for exports, online checks and joins.

classify() turns a finished process into an Outcome before any output is read for a verdict:

| outcome  | when                                                                                   |
|----------|----------------------------------------------------------------------------------------|
| timeout  | the driver killed the group (runner.Result.timed_out)                                  |
| refused  | exit 2 (a program's `usage:`, a tool's refusal or usage error, conf-join's refusal);    |
|          | a JMC `FAILED:` shape error (§7); TraceForge "configuration is refused"; any other     |
|          | non-zero exit; exit 0 whose output does not parse                                      |
| failure  | Rust exit 101 (a panic); TraceForge online exit 1 with `FAILED:`; JMC `FAILED:` with   |
|          | any other throwable (assertions included)                                               |
| ok       | exit 0 (conf-join: 0 or 1), output parsed by the adapter (§4.2 decides)                 |

side_verdict() is the table's two columns: on the reference side every non-ok outcome but a timeout
is refused; on the checked side a program failure is a violation (the message is the witness).
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field

from . import errors

OK, REFUSED, FAILURE, TIMEOUT = "ok", "refused", "failure", "timeout"
TOOLS = ("tf", "jmc", "conf-join")
STEPS = ("export", "online", "join")

# TraceForge's own usage text (program.rs USAGE, first line): a refusal of the driver's command line.
TF_USAGE = "usage: <bin> count spec|impl"
CONF_JOIN_USAGE = "usage: conf-join"
JMC_USAGE = "usage: MpLauncher"
# The throwable's type at its place in the diagnostic, not words in a message.
# Review S1-S6 A3 (D4 rejected): only a launcher-level OutOfMemoryError, outside any process (MpLauncher's
# run() catch prints the bare throwable), is refused; one raised in a process is the program's failure.
_OOM = re.compile(r"java\.lang\.OutOfMemoryError\b")
_TF_ASSERT = re.compile(r"^\S+: export refused: the program raised \d+ assertion failure\(s\)")
_PANIC = re.compile(r"thread '.*' (?:\(\d+\) )?panicked at (.*)")


@dataclass
class Outcome:
    kind: str  # ok | refused | failure | timeout
    message: str  # the deciding line, verbatim ("" for ok)
    category: str | None = None  # errors.CATEGORIES key
    reason: str = ""  # a short machine-readable reason (usage, tool-usage, shape, panic, ...)
    exit: int | None = None
    data: dict = field(default_factory=dict)  # what the adapter parsed (ok outcomes)

    @property
    def ok(self) -> bool:
        return self.kind == OK


def _lines(text: str) -> list:
    return text.splitlines()


def _first(text: str, pred) -> str | None:
    for l in _lines(text):
        if pred(l):
            return l
    return None


def _refusal_category(text: str) -> str:
    """The category of a refusal message (any tool)."""
    t = text
    if "models differ" in t or "uses model" in t or "unknown model" in t or "model " in t and "differ" in t:
        return "model"
    if "`visible`" in t or "visible list" in t or "roles lists differ" in t or "roles were not compared" in t:
        return "visible"
    if "not `complete`" in t or "not complete" in t:
        return "budgets"
    if "invalid declared visible name" in t or "declared visible name is a daemon" in t or \
            "visible name is declared twice" in t:
        return "roles"
    if "assertion failure" in t:
        return "failures"
    cat = errors.jmc_shape_category(t)
    if cat:
        return cat
    return "contributing"


def classify(tool: str, step: str, exit_code: int, timed_out: bool, stdout: str, stderr: str) -> Outcome:
    """§4.1 for one finished process. `tool` is tf (a Rust program's binary), jmc (MpLauncher) or
    conf-join (or witness-join.sh, which keeps conf-join's exit code)."""
    if tool not in TOOLS or step not in STEPS:
        raise ValueError(f"classify: tool {tool}, step {step}")
    if timed_out:
        return Outcome(TIMEOUT, "the driver timed the process out (SIGTERM to its group)", "budgets", "timeout",
                       exit_code)
    if tool == "conf-join":
        if exit_code in (0, 1):
            return Outcome(OK, "", exit=exit_code)
        if exit_code == 2:
            usage = _first(stderr, lambda l: l.startswith(CONF_JOIN_USAGE) or "takes a value" in l
                           or "need --witness-dir" in l or "takes N" in l or "takes a non-negative" in l)
            if usage:
                return Outcome(REFUSED, usage, "contributing", "tool-usage", exit_code)
            msg = _first(stderr, lambda l: "refused" in l) or _first(stderr, bool) or "conf-join exit 2"
            msg = msg.removeprefix("witness-join: conf-join refused (exit 2): ").strip()
            return Outcome(REFUSED, msg, _refusal_category(msg), "join-refused", exit_code)
        return _other(exit_code, stderr)

    if exit_code == 0:
        return Outcome(OK, "", exit=exit_code)

    if exit_code == 2:
        if tool == "tf":
            # Coordinator's ruling (review S1-S6 F3): a Rust assertion in the program is a program failure in
            # both modes; `export` reports it with exit 2 (program.rs:761).
            am = _first(stderr, lambda l: _TF_ASSERT.search(l) is not None)
            if am:
                return Outcome(FAILURE, am, "failures", "assertion", exit_code)
            if "the configuration is refused" in stderr:
                msg = _first(stderr, lambda l: "the configuration is refused" in l)
                return Outcome(REFUSED, msg, "contributing", "configuration", exit_code)
            # The program's own usage line (the usage convention) unless it is TraceForge's usage text.
            prog_usage = _first(stderr, lambda l: l.startswith("usage:") and not l.startswith(TF_USAGE))
            tool_usage = TF_USAGE in stderr
            if prog_usage and not tool_usage:
                detail = _first(stderr, lambda l: l and not l.startswith("usage:"))
                msg = f"{detail}; {prog_usage}" if detail else prog_usage
                return Outcome(REFUSED, msg, "args", "usage", exit_code)
            msg = _first(stderr, lambda l: l and not l.startswith("usage:") and not l.startswith(" ")) \
                or "exit 2"
            if tool_usage and not any(k in msg for k in ("refused", "not `complete`")):
                return Outcome(REFUSED, msg, "contributing", "tool-usage", exit_code)
            return Outcome(REFUSED, msg, _refusal_category(msg), "tool-refused", exit_code)
        # JMC: MpLauncher's usage() / constructionFailure() (exit 2)
        msg = _first(stderr, lambda l: l.startswith("MpLauncher: ")) or _first(stderr, bool) or "exit 2"
        if "cannot load and construct" in msg:
            if "IllegalArgumentException" in msg:
                return Outcome(REFUSED, msg, "args", "usage", exit_code)
            return Outcome(REFUSED, msg, "args", "construct", exit_code)
        if "specification" in msg and "refused" in msg:
            return Outcome(REFUSED, msg, _refusal_category(msg), "tool-refused", exit_code)
        return Outcome(REFUSED, msg, "contributing", "tool-usage", exit_code)

    if tool == "tf" and exit_code == 101:
        m = None
        lines = _lines(stderr)
        for k, l in enumerate(lines):
            pm = _PANIC.match(l)
            if pm:
                nxt = lines[k + 1] if k + 1 < len(lines) else ""
                m = f"panicked at {pm.group(1)} {nxt}".strip()
                break
        return Outcome(FAILURE, m or "exit 101 (a panic)", "failures", "panic", exit_code)

    if exit_code == 1:
        failed = _first(stdout, lambda l: l.startswith("FAILED:"))
        if failed is not None:
            msg = failed[len("FAILED:"):].strip()
            if tool == "tf" and step == "online":
                return Outcome(FAILURE, msg, "failures", "online-failed", exit_code)
            if tool == "jmc":
                cat = errors.jmc_shape_category(msg)
                if cat:
                    return Outcome(REFUSED, msg, cat, "shape", exit_code)
                if _launcher_output_failure(msg):
                    # The launcher's own output failed (MpLauncher.java:660-680, 1027, 1034: export, store,
                    # witnesses), also when it follows a program failure: refused, never a violation (D4).
                    return Outcome(REFUSED, msg, "contributing", "output-failed", exit_code)
                if _OOM.match(msg):
                    # A resource limit, not the program's behaviour (DRIVER-IMPL-NOTES D4).
                    return Outcome(REFUSED, msg, "budgets", "out-of-memory", exit_code)
                return Outcome(FAILURE, msg, "failures", "jmc-failed", exit_code)
    return _other(exit_code, stderr)


def _launcher_output_failure(msg: str) -> bool:
    """MpLauncher's own output failures (`cannot write <file>: ...`, `cannot write witness ...`, `cannot write
    the witness of iteration ...`, MpLauncher.java:660-680, 1027, 1034): the whole message, or a segment it
    appended after a program failure (`<failure>; cannot write ...`) is decided by the outputs
    (appended_output_failure, jmc.export_outcome). A throwable's own text that contains the words is the
    program's."""
    return msg.startswith("cannot write ")


def appended_output_failure(msg: str) -> bool:
    """A `; cannot write ...` segment after a program failure: the launcher's, or the throwable's own text.
    Only the outputs can tell (jmc.export_outcome: no export file means the launcher's write failed)."""
    return any(p.startswith("cannot write ") for p in msg.split("; ")[1:])


def _other(exit_code: int, stderr: str) -> Outcome:
    if exit_code < 0:
        msg = f"killed by signal {-exit_code}"
    elif exit_code == 127:
        msg = _first(stderr, bool) or "exit 127 (not started)"
    else:
        msg = f"exit {exit_code}"
        last = [l for l in _lines(stderr) if l.strip()]
        if last:
            msg += f": {last[-1].strip()}"
    return Outcome(REFUSED, msg, "contributing", "other-exit", exit_code)


def side_verdict(o: Outcome, role: str) -> str | None:
    """§4.1's columns: None for ok (§4.2 decides), else the direction's verdict. `role` is reference
    (the spec side of the direction) or checked (the impl or mutant side)."""
    if role not in ("reference", "checked"):
        raise ValueError(role)
    if o.kind == OK:
        return None
    if o.kind == TIMEOUT:
        return "incomplete"
    if o.kind == FAILURE and role == "checked":
        return "violation"
    return "refused"
