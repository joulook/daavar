"""Direction verdicts and aggregation (DRIVER-DESIGN §4.2, §4.3, §4.4): the only truth table.

Inputs are already-classified outcomes (§4.1, outcome.py): the reference export, the checked export (offline
or online) and the join. A §4.1 failure replaces the §4.2 row: a program failure on the checked side is a
violation, on the reference side the direction is refused; a timeout is incomplete.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from . import outcome

ORDER = {"refused": 3, "violation": 2, "incomplete": 1, "conforms": 0}
EXIT = {"refused": 2, "violation": 1, "incomplete": 2, "conforms": 0}
STOP_FA = "stopped at a false alarm; rerun without --stop-at-first"
STOP_UNRES = "stopped at an unresolved summary; rerun without --stop-at-first"


@dataclass
class DirVerdict:
    status: str  # refused | violation | incomplete | conforms (before the mutant mapping)
    note: str = ""
    reason: str = ""  # which row decided: ref-failure, checked-failure, ref-incomplete, join-refused, decide, ...
    message: str = ""  # the tool's message for a failure row
    category: str | None = None

    def gated(self, form: str) -> str:
        """§4.3's mutant mapping: a mutant's violation is "rejected" (conforms), its conformance "not rejected"
        (incomplete); every other status stands."""
        if form == "mutant<=reference":
            return {"violation": "conforms", "conforms": "incomplete"}.get(self.status, self.status)
        return self.status

    def label(self, form: str) -> str:
        if form == "mutant<=reference":
            return {"violation": "rejected", "conforms": "not rejected"}.get(self.status, self.status)
        return self.status


def direction(ref, checked, join) -> DirVerdict:
    """`ref` and `checked` are export Outcomes (data: export, stopped_early, ...); `join` the join Outcome or
    None (not run). §4.1 first, then §4.2."""
    rv = outcome.side_verdict(ref, "reference")
    cv = outcome.side_verdict(checked, "checked") if checked is not None else None
    # Both sides are classified (reviews S7-S10: Astra 3, Opus F1): the worse verdict by §4.3's order decides,
    # the reference winning ties; the other side is kept in the note. A reference timeout never hides a
    # checked-side failure or refusal, for either tool.
    if rv is not None or cv is not None:
        rvx = DirVerdict(rv, f"reference {ref.kind}", "ref-" + ref.kind, ref.message, ref.category) if rv else None
        cvx = (DirVerdict(cv, f"checked side {checked.kind}", "checked-" + checked.kind, checked.message,
                          checked.category) if cv else None)
        if rvx and cvx:
            win, other = (rvx, cvx) if ORDER[rv] >= ORDER[cv] else (cvx, rvx)
            win.note += f"; also {other.note} ({other.status})"
            return win
        return rvx or cvx
    if not ref.data["export"].complete:
        return DirVerdict("incomplete", f"the reference export is {ref.data['export'].enumeration}", "ref-incomplete",
                          category="budgets")
    if checked is None:
        return DirVerdict("refused", "no checked export", "no-export", category="contributing")
    if join is None:
        return DirVerdict("refused", "no join", "no-join", category="contributing")
    if not join.ok:
        # §4.1: a timed-out join is incomplete (Astra 2), any other join failure refused.
        st = "incomplete" if join.kind == outcome.TIMEOUT else "refused"
        return DirVerdict(st, f"join {join.kind}", "join-" + join.kind, join.message, join.category)
    rep = join.data["report"]
    d = rep.decide
    if d["violations"] > 0:
        return DirVerdict("violation", f"{d['violations']} Decide-confirmed violation(s)", "decide")
    if checked.data.get("stopped_early"):
        if d["false_alarms"] > 0:
            return DirVerdict("incomplete", STOP_FA, "stopped")
        if d["unresolved"] > 0:
            return DirVerdict("incomplete", STOP_UNRES, "stopped")
        return DirVerdict("incomplete", "stopped early", "stopped")
    if not checked.data["export"].complete:
        return DirVerdict("incomplete", f"the checked export is {checked.data['export'].enumeration}", "checked-incomplete")
    if rep.uncovered == 0:
        return DirVerdict("conforms", "", "decide")
    if d["unresolved"] > 0:
        return DirVerdict("incomplete", f"{d['unresolved']} unresolved", "decide")
    return DirVerdict("conforms", f"{d['false_alarms']} false alarms", "decide")


def worst(statuses) -> str | None:
    """Within one direction (§4.1/§4.2): refused > violation > incomplete > conforms."""
    s = list(statuses)
    return max(s, key=lambda x: ORDER[x]) if s else None


def run_status(statuses) -> str | None:
    """Across gating directions (coordinator's rule, final review 1; DRIVER-IMPL-NOTES D23): a violation
    anywhere decides (exit 1), else refused, else incomplete (exit 2), else conforms (exit 0). A violation is never
    hidden by another direction's refusal."""
    s = set(statuses)
    if not s:
        return None
    for st in ("violation", "refused", "incomplete", "conforms"):
        if st in s:
            return st


@dataclass
class Aggregate:
    status: str  # CONFORMS | VIOLATION | INCOMPLETE | REFUSED | REPORT ONLY
    exit: int
    gating: str | None  # the gating status (lower case) or None
    all: str | None  # over every reported direction
    cells: dict = field(default_factory=dict)  # cell name -> gating status or None


def aggregate(cells) -> Aggregate:
    """`cells`: [(cell name, [(Direction, DirVerdict), ...])]. Precedence over a cell's gating directions,
    then over the cells; no gating direction at all is REPORT ONLY (exit 2)."""
    per, alls, gall = {}, [], []
    for name, dirs in cells:
        g = [v.gated(d.form) for d, v in dirs if d.gating]
        per[name] = run_status(g)
        gall += g
        alls += [v.gated(d.form) for d, v in dirs]
    gating = run_status(gall)  # over every gating direction of every cell, not over the cells' summaries
    allst = run_status(alls)
    if gating is None:
        return Aggregate("REPORT ONLY", 2, None, allst, per)
    return Aggregate(gating.upper(), EXIT[gating], gating, allst, per)
