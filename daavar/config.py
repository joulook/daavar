"""daavar.toml: loading, static validation, cell expansion and cell selection (DRIVER-DESIGN §2).

Expansion. A program's `args` items are templates over the cell's fields, then the scenario's fields
and tables (`{fam.spec}`, `{recancel}{split}`). Items that expand to "" are dropped and the rest are
comma-joined: one Rust ARGS argument, or the Java `Class:items` argument. No empty argument results.

Cells. A scenario's cells are its `cells` tables (in order), then the product of its `matrix` field
lists (the first field outermost), each with the `label` template. Labels are distinct per scenario.
"""

from __future__ import annotations

import itertools
import re
import tomllib
from dataclasses import dataclass, field
from pathlib import Path

from . import DaavarError


class ConfigError(DaavarError):
    """A static validation error of daavar.toml (exit 2)."""


MODELS = ("fifo", "bag", "causal")
KINDS = ("spec", "impl", "mutant")
LANGS = ("rust", "java")
TOP_KEYS = ("name", "model", "programs", "scenarios", "check", "budgets", "classify", "vocab", "tools")
PROGRAM_KEYS = ("kind", "lang", "side", "class", "args", "cells", "reference")
SCENARIO_RESERVED = ("visible", "programs", "directions", "cells", "matrix")
CHECK_DEFAULTS = {
    "mode": "offline",
    "seed": 1,
    "threads": 1,
    "decide_budget": 1000000,
    "witness_limit": 20,
    "witness_nearest": 3,
    "merge": False,
    "stop_at_first": False,
}
BUDGET_DEFAULTS = {
    "export_timeout_s": 3600,
    "join_timeout_s": 3600,
    "java_xmx": "16g",
    "mem_cap_gb": 0,
}
CLASSIFY_KEYS = ("command",)
VOCAB_KEYS = ("roots", "rust_only", "java_only", "opaque")
JMC_KEYS = ("checkout", "cli", "main", "api_package")
JMC_CLI = ("positional", "named", "auto")

# A placeholder: {field} or {table.key}.
_PLACEHOLDER = re.compile(r"\{([^{}]*)\}")
_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_-]*\Z")


@dataclass(frozen=True)
class Program:
    id: str
    kind: str
    lang: str
    args: tuple[str, ...]
    side: str | None = None
    cls: str | None = None
    cells: tuple[str, ...] | None = None
    reference: str | None = None


@dataclass(frozen=True)
class Direction:
    """`checked<=ref`. `form` is impl<=spec, mutant<=reference or impl<=impl; only the first two gate."""

    checked: str
    ref: str
    form: str
    gating: bool

    @property
    def name(self) -> str:
        return f"{self.checked}<={self.ref}"


@dataclass(frozen=True)
class Cell:
    scenario: str
    label: str
    fields: dict  # the cell's own fields (str -> str)


@dataclass
class Scenario:
    id: str
    visible: tuple[str, ...]
    programs: tuple[str, ...]
    directions: tuple[Direction, ...]
    scalars: dict  # shared fields (str -> str)
    tables: dict  # shared tables (str -> {str: str})
    cells: list  # list[Cell], in expansion order

    def effective_fields(self, cell: Cell) -> dict:
        """The scenario's scalar fields overridden by the cell's: what the field filters see."""
        f = dict(self.scalars)
        f.update(cell.fields)
        return f


@dataclass(frozen=True)
class Invocation:
    """One program in one cell: the expanded, non-empty argument items."""

    program: Program
    items: tuple[str, ...]

    @property
    def joined(self) -> str:
        return ",".join(self.items)

    @property
    def argv_text(self) -> str:
        """The program's argument as a tool gets it: the Rust ARGS, or Java `Class[:items]`."""
        if self.program.lang == "java":
            return f"{self.program.cls}:{self.joined}" if self.items else self.program.cls
        return self.joined


@dataclass
class SelectedCell:
    scenario: Scenario
    cell: Cell
    invocations: dict  # program id -> Invocation, active programs only, in scenario order
    directions: list  # list[Direction] kept (both programs active)

    @property
    def name(self) -> str:
        return f"{self.scenario.id}.{self.cell.label}"


@dataclass
class Config:
    root: Path  # the project directory (where daavar.toml is)
    path: Path
    name: str
    model: str
    programs: dict  # id -> Program, in file order
    scenarios: dict  # id -> Scenario, in file order
    check: dict
    budgets: dict
    classify: dict
    vocab: dict
    tools: dict
    raw: dict = field(repr=False, default_factory=dict)

    def tool_path(self, rel: str) -> Path:
        return (self.root / rel).resolve() if not Path(rel).is_absolute() else Path(rel)

    @property
    def langs(self) -> set:
        return {p.lang for p in self.programs.values()}


# ---------------------------------------------------------------- loading and validation


def _err(where: str, msg: str) -> ConfigError:
    return ConfigError(f"daavar.toml: {where}: {msg}")


def _scalar(where: str, v) -> str:
    if isinstance(v, bool) or not isinstance(v, (str, int)):
        raise _err(where, f"must be a string, not {type(v).__name__} {v!r}")
    return str(v)


def _str_list(where: str, v, allow_empty: bool = False) -> tuple[str, ...]:
    if not isinstance(v, list) or not all(isinstance(x, str) for x in v):
        raise _err(where, f"must be a list of strings, not {v!r}")
    if not v and not allow_empty:
        raise _err(where, "must not be empty")
    return tuple(v)


def _unknown_keys(where: str, d: dict, allowed) -> None:
    extra = [k for k in d if k not in allowed]
    if extra:
        raise _err(where, f"unknown key(s) {', '.join(extra)}; valid: {', '.join(allowed)}")


def parse_template(where: str, template: str) -> list:
    """The placeholders of a template, as (name, key-or-None), validated."""
    rest = _PLACEHOLDER.sub("", template)
    if "{" in rest or "}" in rest:
        raise _err(where, f"unbalanced brace in template {template!r}")
    out = []
    for m in _PLACEHOLDER.finditer(template):
        parts = m.group(1).split(".")
        if len(parts) > 2 or not all(_NAME.match(p) for p in parts):
            raise _err(where, f"bad placeholder {{{m.group(1)}}} in {template!r} (use {{field}} or {{table.key}})")
        out.append((parts[0], parts[1] if len(parts) == 2 else None))
    return out


def expand(template: str, cell_fields: dict, scalars: dict, tables: dict, where: str) -> str:
    """One template expanded: cell fields first, then the scenario's fields and tables."""
    parse_template(where, template)

    def sub(m: re.Match) -> str:
        parts = m.group(1).split(".")
        name = parts[0]
        if len(parts) == 2:
            if name in cell_fields or name in scalars:
                raise _err(where, f"{{{m.group(1)}}}: {name} is a field, not a table")
            if name not in tables:
                raise _err(where, f"{{{m.group(1)}}}: no table {name}; tables: {_names(tables)}")
            if parts[1] not in tables[name]:
                raise _err(where, f"{{{m.group(1)}}}: table {name} has no key {parts[1]}; keys: {_names(tables[name])}")
            return tables[name][parts[1]]
        if name in cell_fields:
            return cell_fields[name]
        if name in scalars:
            return scalars[name]
        if name in tables:
            raise _err(where, f"{{{name}}} is a table; name a key ({{{name}.<key>}})")
        valid = sorted(set(cell_fields) | set(scalars))
        raise _err(where, f"{{{name}}} does not resolve; fields: {', '.join(valid) or '(none)'}")

    return _PLACEHOLDER.sub(sub, template)


def _names(d) -> str:
    return ", ".join(d) if d else "(none)"


def load(path: str | Path) -> Config:
    """Reads and validates a daavar.toml (a file, or a directory holding one)."""
    p = Path(path)
    if p.is_dir():
        p = p / "daavar.toml"
    try:
        with open(p, "rb") as f:
            raw = tomllib.load(f)
    except FileNotFoundError:
        raise ConfigError(f"no daavar.toml at {p}") from None
    except tomllib.TOMLDecodeError as e:
        raise ConfigError(f"{p}: not TOML: {e}") from None
    return from_dict(raw, p.resolve().parent, p)


def from_dict(raw: dict, root: Path, path: Path | None = None) -> Config:
    _unknown_keys("top level", raw, TOP_KEYS)
    for k in ("name", "model", "programs", "scenarios", "tools"):
        if k not in raw:
            raise _err("top level", f"missing key {k}")
    name = _scalar("name", raw["name"])
    model = raw["model"]
    if model not in MODELS:
        raise _err("model", f"{model!r} is not one of {', '.join(MODELS)}")

    programs = {}
    if not isinstance(raw["programs"], dict) or not raw["programs"]:
        raise _err("programs", "needs at least one [programs.<id>] table")
    for pid, pd in raw["programs"].items():
        programs[pid] = _program(pid, pd)
    for p in programs.values():
        if p.kind == "mutant":
            ref = programs.get(p.reference)
            if ref is None:
                raise _err(f"programs.{p.id}.reference", f"{p.reference!r} names no program; programs: {_names(programs)}")
            if ref.kind == "mutant":
                raise _err(f"programs.{p.id}.reference", f"{p.reference} is itself a mutant")
    if model == "causal":
        java = [p.id for p in programs.values() if p.lang == "java"]
        if java:
            raise _err("model", f"causal is Rust-only (JMC's Must supports fifo and bag); Java programs: {', '.join(java)}")

    scenarios = {}
    if not isinstance(raw["scenarios"], dict) or not raw["scenarios"]:
        raise _err("scenarios", "needs at least one [scenarios.<id>] table")
    for sid, sd in raw["scenarios"].items():
        scenarios[sid] = _scenario(sid, sd, programs)

    # A program's `cells` must name labels of scenarios that list it.
    for p in programs.values():
        if p.cells is None:
            continue
        labels = {c.label for s in scenarios.values() if p.id in s.programs for c in s.cells}
        bad = [c for c in p.cells if c not in labels]
        if bad:
            raise _err(f"programs.{p.id}.cells", f"unknown label(s) {', '.join(bad)}; labels: {', '.join(sorted(labels)) or '(none)'}")

    # Every program of every cell expands, and no argument is empty.
    for s in scenarios.values():
        for c in s.cells:
            for pid in s.programs:
                if active(programs[pid], c.label):
                    invocation(programs[pid], s, c)

    check = dict(CHECK_DEFAULTS)
    check.update(_table("check", raw.get("check", {}), CHECK_DEFAULTS))
    if check["mode"] not in ("offline", "online"):
        raise _err("check.mode", f"{check['mode']!r} is not offline or online")
    for k in ("seed", "threads", "decide_budget", "witness_limit", "witness_nearest"):
        if isinstance(check[k], bool) or not isinstance(check[k], int) or check[k] < 0:
            raise _err(f"check.{k}", f"must be a non-negative integer, not {check[k]!r}")
    if check["threads"] < 1:
        raise _err("check.threads", "must be at least 1")
    for k in ("merge", "stop_at_first"):
        if not isinstance(check[k], bool):
            raise _err(f"check.{k}", f"must be true or false, not {check[k]!r}")
    budgets = dict(BUDGET_DEFAULTS)
    budgets.update(_table("budgets", raw.get("budgets", {}), BUDGET_DEFAULTS))
    for k in ("export_timeout_s", "join_timeout_s", "mem_cap_gb"):
        if isinstance(budgets[k], bool) or not isinstance(budgets[k], int) or budgets[k] < 0:
            raise _err(f"budgets.{k}", f"must be a non-negative integer, not {budgets[k]!r}")
    if budgets["export_timeout_s"] < 1 and any(p.lang == "java" for p in programs.values()):
        raise _err("budgets.export_timeout_s", "must be at least 1 with Java programs (MpLauncher's timeoutSeconds, "
                   "MpLauncher.java:379)")
    if not isinstance(budgets["java_xmx"], str) or not re.fullmatch(r"[0-9]+[kKmMgG]?", budgets["java_xmx"]):
        raise _err("budgets.java_xmx", f"must be a JVM size such as 16g, not {budgets['java_xmx']!r}")

    classify = raw.get("classify", {})
    _unknown_keys("classify", classify, CLASSIFY_KEYS)
    if "command" in classify:
        _scalar("classify.command", classify["command"])
    vocab = raw.get("vocab", {})
    _unknown_keys("vocab", vocab, VOCAB_KEYS)
    vocab = {k: _str_list(f"vocab.{k}", vocab.get(k, []), allow_empty=True) for k in VOCAB_KEYS}
    if not vocab["roots"]:
        vocab["roots"] = None  # not given: derived from msgs.rs (§3.4)
    tools = _tools(raw["tools"], programs)
    return Config(root=root, path=path or root / "daavar.toml", name=name, model=model, programs=programs,
                  scenarios=scenarios, check=check, budgets=budgets, classify=classify, vocab=vocab,
                  tools=tools, raw=raw)


def _table(where: str, d, defaults: dict) -> dict:
    if not isinstance(d, dict):
        raise _err(where, "must be a table")
    _unknown_keys(where, d, defaults)
    return d


def _tools(d, programs: dict) -> dict:
    if not isinstance(d, dict):
        raise _err("tools", "must be a table")
    _unknown_keys("tools", d, ("traceforge", "jmc"))
    if "traceforge" not in d:
        raise _err("tools", "missing key traceforge (the TraceForge checkout: conf-join joins every direction)")
    out = {"traceforge": _scalar("tools.traceforge", d["traceforge"]), "jmc": None}
    j = d.get("jmc")
    if j is None:
        if any(p.lang == "java" for p in programs.values()):
            raise _err("tools", "missing [tools.jmc] (the project has Java programs)")
        return out
    if not isinstance(j, dict):
        raise _err("tools.jmc", "must be a table")
    _unknown_keys("tools.jmc", j, JMC_KEYS)
    for k in ("checkout", "main"):
        if k not in j:
            raise _err("tools.jmc", f"missing key {k}")
    jm = {k: _scalar(f"tools.jmc.{k}", j[k]) for k in j}
    jm.setdefault("cli", "auto")
    jm.setdefault("api_package", "org.mpi_sws.jmc.mp.launch")
    if jm["cli"] not in JMC_CLI:
        raise _err("tools.jmc.cli", f"{jm['cli']!r} is not one of {', '.join(JMC_CLI)}")
    out["jmc"] = jm
    return out


def _program(pid: str, pd) -> Program:
    where = f"programs.{pid}"
    if not isinstance(pd, dict):
        raise _err(where, "must be a table")
    _unknown_keys(where, pd, PROGRAM_KEYS)
    for k in ("kind", "lang", "args"):
        if k not in pd:
            raise _err(where, f"missing key {k}")
    kind, lang = pd["kind"], pd["lang"]
    if kind not in KINDS:
        raise _err(f"{where}.kind", f"{kind!r} is not one of {', '.join(KINDS)}")
    if lang not in LANGS:
        raise _err(f"{where}.lang", f"{lang!r} is not one of {', '.join(LANGS)}")
    args = _str_list(f"{where}.args", pd["args"], allow_empty=True)
    for i, a in enumerate(args):
        parse_template(f"{where}.args[{i}]", a)
    side = cls = None
    if lang == "rust":
        if "class" in pd:
            raise _err(where, "a Rust program names a side, not a class")
        side = pd.get("side")
        if side not in ("spec", "impl"):
            raise _err(f"{where}.side", f"a Rust program needs side = \"spec\" or \"impl\", not {side!r}")
    else:
        if "side" in pd:
            raise _err(where, "a Java program names a class, not a side")
        cls = pd.get("class")
        if not isinstance(cls, str) or not re.fullmatch(r"[A-Za-z_$][\w$]*(\.[A-Za-z_$][\w$]*)*", cls):
            raise _err(f"{where}.class", f"a Java program needs a class name, not {cls!r}")
    reference = None
    if kind == "mutant":
        reference = pd.get("reference")
        if not isinstance(reference, str):
            raise _err(where, "a mutant needs reference = \"<program>\"")
    elif "reference" in pd:
        raise _err(where, "only a mutant has a reference")
    cells = None
    if "cells" in pd:
        cells = _str_list(f"{where}.cells", pd["cells"])
    return Program(id=pid, kind=kind, lang=lang, args=args, side=side, cls=cls, cells=cells, reference=reference)


def parse_direction(where: str, d: str, programs: dict, scenario_programs) -> Direction:
    if not isinstance(d, str) or d.count("<=") != 1:
        raise _err(where, f"{d!r} is not checked<=reference")
    checked, ref = (x.strip() for x in d.split("<="))
    for x in (checked, ref):
        if x not in scenario_programs:
            raise _err(where, f"{d}: {x} is not a program of this scenario; programs: {', '.join(scenario_programs)}")
    if checked == ref:
        raise _err(where, f"{d}: a program against itself")
    c, r = programs[checked], programs[ref]
    if c.kind == "mutant":
        if ref != c.reference:
            raise _err(where, f"{d}: mutant {checked} is checked against its declared reference {c.reference} only")
        return Direction(checked, ref, "mutant<=reference", True)
    if c.kind == "impl" and r.kind == "spec":
        return Direction(checked, ref, "impl<=spec", True)
    if c.kind == "impl" and r.kind == "impl":
        return Direction(checked, ref, "impl<=impl", False)
    raise _err(where, f"{d}: {c.kind}<={r.kind} is not a direction (impl<=spec, mutant<=its reference, impl<=impl)")


def _scenario(sid: str, sd, programs: dict) -> Scenario:
    where = f"scenarios.{sid}"
    if not isinstance(sd, dict):
        raise _err(where, "must be a table")
    for k in ("visible", "programs", "directions"):
        if k not in sd:
            raise _err(where, f"missing key {k}")
    visible = _str_list(f"{where}.visible", sd["visible"])
    if len(set(visible)) != len(visible):
        raise _err(f"{where}.visible", "names are not distinct")
    progs = _str_list(f"{where}.programs", sd["programs"])
    for p in progs:
        if p not in programs:
            raise _err(f"{where}.programs", f"{p} names no program; programs: {_names(programs)}")
    if len(set(progs)) != len(progs):
        raise _err(f"{where}.programs", "names are not distinct")
    dirs_raw = sd["directions"]
    if not isinstance(dirs_raw, list) or not dirs_raw:
        raise _err(f"{where}.directions", "must be a non-empty list")
    directions = tuple(parse_direction(f"{where}.directions", d, programs, progs) for d in dirs_raw)
    names = [d.name for d in directions]
    if len(set(names)) != len(names):
        raise _err(f"{where}.directions", "a direction is listed twice")
    for p in progs:
        pr = programs[p]
        if pr.kind == "mutant" and pr.reference not in progs:
            raise _err(where, f"mutant {p}'s reference {pr.reference} is not a program of this scenario")
    scalars, tables = {}, {}
    for k, v in sd.items():
        if k in SCENARIO_RESERVED:
            continue
        if not _NAME.match(k):
            raise _err(where, f"bad field name {k!r}")
        if isinstance(v, dict):
            tables[k] = {kk: _scalar(f"{where}.{k}.{kk}", vv) for kk, vv in v.items()}
        else:
            scalars[k] = _scalar(f"{where}.{k}", v)
    cells = []
    for i, cd in enumerate(sd.get("cells", [])):
        cw = f"{where}.cells[{i}]"
        if not isinstance(cd, dict):
            raise _err(cw, "must be a table")
        if "label" not in cd:
            raise _err(cw, "missing key label")
        f = {}
        for k, v in cd.items():
            if k == "label":
                continue
            if not _NAME.match(k):
                raise _err(cw, f"bad field name {k!r}")
            if k in tables:
                raise _err(cw, f"field {k} shadows the scenario's table {k}")
            f[k] = _scalar(f"{cw}.{k}", v)
        label = expand(_scalar(f"{cw}.label", cd["label"]), f, scalars, tables, f"{cw}.label")
        cells.append(Cell(sid, label, f))
    if "matrix" in sd:
        md = sd["matrix"]
        mw = f"{where}.matrix"
        if not isinstance(md, dict):
            raise _err(mw, "must be a table")
        if "label" not in md:
            raise _err(mw, "missing key label (a template over the matrix fields)")
        keys = [k for k in md if k != "label"]
        if not keys:
            raise _err(mw, "needs at least one field list")
        lists = []
        for k in keys:
            if not _NAME.match(k):
                raise _err(mw, f"bad field name {k!r}")
            if k in tables:
                raise _err(mw, f"field {k} shadows the scenario's table {k}")
            v = md[k]
            if not isinstance(v, list) or not v:
                raise _err(f"{mw}.{k}", "must be a non-empty list")
            lists.append([_scalar(f"{mw}.{k}", x) for x in v])
        label_t = _scalar(f"{mw}.label", md["label"])
        for combo in itertools.product(*lists):
            f = dict(zip(keys, combo))
            cells.append(Cell(sid, expand(label_t, f, scalars, tables, f"{mw}.label"), f))
    if not cells:
        raise _err(where, "has no cells (give cells and/or a matrix)")
    seen = set()
    for c in cells:
        if c.label in seen:
            raise _err(where, f"label {c.label!r} is not distinct")
        if not c.label or not re.fullmatch(r"[A-Za-z0-9_.+-]+", c.label):
            raise _err(where, f"label {c.label!r} must be non-empty and use letters, digits and . _ + -")
        seen.add(c.label)
    return Scenario(id=sid, visible=visible, programs=progs, directions=directions, scalars=scalars,
                    tables=tables, cells=cells)


def invocation(p: Program, s: Scenario, c: Cell) -> Invocation:
    items = []
    for i, t in enumerate(p.args):
        v = expand(t, c.fields, s.scalars, s.tables, f"programs.{p.id}.args[{i}] in {s.id}.{c.label}")
        if v != "":
            if "," in v:
                raise _err(f"programs.{p.id}.args[{i}] in {s.id}.{c.label}",
                           f"item {v!r} holds a comma (items are comma-joined)")
            items.append(v)
    return Invocation(p, tuple(items))


def active(p: Program, label: str) -> bool:
    return p.cells is None or label in p.cells


# ---------------------------------------------------------------- selection


def _show(values) -> str:
    return ", ".join('""' if v == "" else v for v in values)


def select(cfg: Config, scenarios=None, cells=None, where=None) -> list:
    """The selected cells: `scenarios` and `cells` are lists of ids and labels; `where` is a list of
    (field, [values]) filters, each keeping the cells whose field takes one of the values. An unknown
    name or value is an error that lists the valid ones."""
    chosen = list(cfg.scenarios.values())
    if scenarios:
        for s in scenarios:
            if s not in cfg.scenarios:
                raise ConfigError(f"unknown scenario {s}; valid: {', '.join(cfg.scenarios)}")
        chosen = [cfg.scenarios[s] for s in cfg.scenarios if s in scenarios]
    pool = [(s, c) for s in chosen for c in s.cells]
    if cells:
        labels = []
        for s, c in pool:
            if c.label not in labels:
                labels.append(c.label)
        for lab in cells:
            if lab not in labels:
                raise ConfigError(f"unknown cell {lab}; valid: {', '.join(labels)}")
        pool = [(s, c) for s, c in pool if c.label in cells]
    # Fields and values are validated against the cells the scenario and cell selection keep, so a
    # value is "unknown" only if no such cell takes it; filters then combine by AND.
    base = list(pool)
    fields = []
    for s, c in base:
        for k in s.effective_fields(c):
            if k not in fields:
                fields.append(k)
    for fname, values in where or []:
        if fname not in fields:
            raise ConfigError(f"unknown field {fname}; valid: {', '.join(fields)}")
        taken = []
        for s, c in base:
            v = s.effective_fields(c).get(fname)
            if v is not None and v not in taken:
                taken.append(v)
        for v in values:
            if v not in taken:
                raise ConfigError(f"unknown value {v!r} of field {fname}; valid: {_show(taken)}")
        pool = [(s, c) for s, c in pool if s.effective_fields(c).get(fname) in values]
    if not pool:
        raise ConfigError("the filters select no cell")
    out = []
    for s, c in pool:
        inv = {pid: invocation(cfg.programs[pid], s, c) for pid in s.programs if active(cfg.programs[pid], c.label)}
        dirs = [d for d in s.directions if d.checked in inv and d.ref in inv]
        out.append(SelectedCell(scenario=s, cell=c, invocations=inv, directions=dirs))
    return out


def parse_where(texts) -> list:
    """`--where field=v1,v2` texts as filters (an empty value is written as `field=`)."""
    out = []
    for t in texts or []:
        if "=" not in t:
            raise ConfigError(f"--where {t!r}: expected <field>=<values>")
        k, v = t.split("=", 1)
        out.append((k, v.split(",")))
    return out
