"""Error categories, guide anchors and fixes (DRIVER-DESIGN §7), and the verbatim failure text.

A failure is shown as `<who>: <class>: <tool message, verbatim>. See docs/<page>#<anchor>. Fix: <hint>.`,
followed by at most 20 lines of the tool's output and the log path.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

EXCERPT_LINES = 20


@dataclass(frozen=True)
class Category:
    name: str
    anchor: str  # docs/<page>#<anchor>
    fix: str


CATEGORIES = {c.name: c for c in (
    Category("harness", "docs/modelling.md#harness",
             "main spawns every process, then awaits all of them, and sends or receives nothing itself; "
             "every non-daemon process is spawned under a declared role"),
    Category("roles", "docs/modelling.md#roles",
             "declare each role once, ASCII, not `main` and not starting with `~`; visible roles must be "
             "spawned roles, with identical names on both sides"),
    Category("identifiers", "docs/modelling.md#identifiers",
             "a visible payload may name main, a visible role or a declared payload-nameable role only"),
    Category("values", "docs/modelling.md#values",
             "send only encodable values: @Tag records / derived-Debug types, integers, booleans, strings, "
             "Optional, List, Set; no floats, maps, arrays or null"),
    Category("model", "docs/running.md#model",
             "use the same communication model on both sides (daavar.toml `model`; Java `model()`)"),
    Category("visible", "docs/running.md#visible",
             "give both programs the same visible roles (and payload-nameable roles)"),
    Category("budgets", "docs/running.md#budgets",
             "the reference export did not complete: raise [budgets] export_timeout_s (or the tool's own "
             "budget), or shrink the cell"),
    Category("failures", "docs/interpreting.md#failures",
             "a program failed (a panic, an assertion, an exception): the message is the witness; on the "
             "reference side the reference itself must be fixed"),
    Category("args", "docs/running.md#args",
             "the program refused its arguments (exit 2 with `usage:`): check the program's args templates "
             "and the cell's fields in daavar.toml"),
    Category("contributing", "docs/CONTRIBUTING.md",
             "a tool refused the driver's command line or wrote no usable output: a driver or tool defect; "
             "report it with driver.log and the command"),
)}

# §7: JMC's own shape diagnostics, recognised by their structure (DRIVER-IMPL-NOTES D3, review S1-S6 A2): a
# MustStrategy / MpValueEncoder refusal message (MustStrategy.java:763-779, 1102, 1179, 1679-1833;
# MpValueEncoder.java:118-280), anchored at the start of the message after JMC's own prefix (`must: ` from
# the strategy, `java.lang.IllegalArgumentException: ` from the launcher catching the strategy's constructor).
# A throwable raised by a program's process or by main carries none of these forms, whatever its text says.
JMC_SHAPE = (
    (r"process\(es\) \[[^\]]*\] were never awaited by main", "harness"),
    (r"process \S+ has no role; ", "harness"),
    (r"main performs a send; the format requires", "harness"),
    (r"main performs a receive; the format requires", "harness"),
    (r"process \S+ was spawned by process \S+; the format requires", "harness"),
    (r"process \S+ was spawned after main's first await; the format requires", "harness"),
    (r"process \S+ awaits another process; the format requires", "harness"),
    (r"task \S+ uses model CD, which must does not support", "model"),
    (r"visible roles must be among the spawned roles\Z", "roles"),
    (r'role "[^"]*" is reserved: ', "roles"),
    (r'role "[^"]*" is not ASCII: ', "roles"),
    (r"roles are not distinct: ", "roles"),
    (r"visible roles are not distinct: ", "roles"),
    (r"declared role \S+ was never spawned", "roles"),
    (r"payload names process ", "identifiers"),
    (r"payload-nameable roles are not distinct: ", "identifiers"),
    (r"payload-nameable role \S+ is (?:not declared|visible)", "identifiers"),
    (r"null is not a value: ", "values"),
    (r"no role name for process ", "identifiers"),
    (r"the role name of process ", "identifiers"),
    (r"two elements of a set encode identically: ", "values"),
    (r"maps are not encodable in format version 1", "values"),
    (r"floating-point values are refused by the format: ", "values"),
    (r"arrays are compared by identity and are refused: ", "values"),
    (r"not encodable: ", "values"),
    (r"string is not valid Unicode ", "values"),
    (r"record \S+ has no @Tag; ", "values"),
    (r"the @Tag of record \S+: ", "values"),
    (r"cannot read component \S+ of ", "values"),
)
_SHAPE_RES = [(re.compile(r"(?:must: |java\.lang\.IllegalArgumentException: )" + rx), cat) for rx, cat in JMC_SHAPE]
_PROCESS_FAILURE = re.compile(r"must: process \S+ \(task \d+\) failed: ")


def category(name: str) -> Category:
    return CATEGORIES[name]


def jmc_shape_category(message: str) -> str | None:
    """The category of a JMC `FAILED:` message that is one of JMC's own shape diagnostics, else None."""
    if _PROCESS_FAILURE.match(message):
        return None
    for rx, cat in _SHAPE_RES:
        if rx.match(message):
            return cat
    return None


def excerpt(text: str, n: int = EXCERPT_LINES) -> list:
    lines = text.splitlines()
    return lines[:n]


def render(who: str, cls: str, message: str, cat: str, log: str | None = None, output: str = "") -> str:
    """The §6 'Refused, incomplete, program failures' entry, with the verbatim excerpt."""
    c = CATEGORIES[cat]
    out = [f"{who}: {cls}: {message.rstrip('.')}. See {c.anchor}. Fix: {c.fix}."]
    ex = excerpt(output)
    if ex:
        out.append("```")
        out.extend(ex)
        if len(output.splitlines()) > len(ex):
            out.append(f"... ({len(output.splitlines()) - len(ex)} more lines)")
        out.append("```")
    if log:
        out.append(f"Log: {log}")
    return "\n".join(out)
